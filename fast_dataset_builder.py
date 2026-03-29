import argparse, base64, json, os, re, sys, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading, socket

# FIX FOR FREEZING: Forcefully timeout any deadlocked GitHub API connection after 20 seconds.
socket.setdefaulttimeout(20)

from ghapi.all import GhApi, paged
from verilog_dataset_builder import PERMISSIVE, VERILOG_EXT, CLOSE_RE, UNLIMITED
from fast_verilog_scraper import queries, check_rate_limit, handle_rate_limit_error

write_lock = threading.Lock()
# We will rely on ghapi passing auth, but we should make sure GhApi rate limits are handled.

def log(msg: str, enabled: bool):
    if enabled:
        print(msg, file=sys.stderr)

def is_verilog_file(path: str) -> bool:
    return path.lower().endswith(VERILOG_EXT)

def load_already_collected(output_path: str) -> Set[str]:
    collected = set()
    p = Path(output_path)
    if not p.exists(): return collected
    with open(output_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rec = json.loads(line)
                    repo = rec.get("repo", "")
                    if repo: collected.add(repo)
                except Exception: pass
    return collected

def fast_search_repositories_with_stars(api: GhApi, debug: bool, token_list: list = None):
    """Fetch repos and include stargazers_count using custom multi-token routing to avoid 30/min limit."""
    seen = set()
    repos_metadata = []
    import urllib.request, threading
    
    lock = threading.Lock()
    token_idx = 0
    
    def get_token():
        nonlocal token_idx
        with lock:
            if not token_list: return api.token
            t = token_list[token_idx % len(token_list)]
            token_idx += 1
            return t

    try:
        for q in queries:
            for page in range(1, 11): # Up to 10 pages per query
                url = f"https://api.github.com/search/repositories?q={urllib.parse.quote(q)}&sort=stars&order=desc&per_page=100&page={page}"
                req = urllib.request.Request(url)
                req.add_header("Authorization", f"token {get_token()}")
                req.add_header("Accept", "application/vnd.github.v3+json")
                
                try:
                    with urllib.request.urlopen(req, timeout=15) as response:
                        data = json.loads(response.read().decode('utf-8'))
                        items = data.get("items", [])
                        if not items: break
                        
                        for it in items:
                            full_name = it.get("full_name")
                            if not full_name or full_name in seen: continue
                            lic = it.get("license") or {}
                            spdx = (lic.get("spdx_id", "") or "").lower()
                            stars = it.get("stargazers_count", 0)
                            
                            if spdx in PERMISSIVE:
                                seen.add(full_name)
                                repos_metadata.append({
                                    'full_name': full_name,
                                    'stars': stars,
                                    'license': {"spdx_id": lic.get("spdx_id", ""), "text": ""}
                                })
                        print(f"[Search Engine] Crawled page {page} for query '{q[:15]}...' (Found {len(items)} repos)", file=sys.stderr)
                except urllib.error.HTTPError as e:
                    if e.code == 403:
                        time.sleep(2) # just pause shortly and try next token on next loop
                        continue
                    if e.code == 422: # Past page 10 logic limit
                        break
                except Exception:
                    time.sleep(1)
    except Exception as e:
        log(f"Search aborted. Exception: {e}", debug)
    return repos_metadata

def get_changed_verilog_files_with_stats(api: GhApi, owner: str, repo: str, prn: int) -> Tuple[List[str], List[Dict]]:
    files, file_stats = [], []
    try:
        for page in paged(api.pulls.list_files, owner, repo, prn, per_page=100):
            for f in page or []:
                fname = getattr(f, "filename", "")
                if is_verilog_file(fname):
                    files.append(fname)
                    file_stats.append({
                        "filename": fname,
                        "status": getattr(f, "status", None),
                        "additions": getattr(f, "additions", 0),
                        "deletions": getattr(f, "deletions", 0),
                        "changes": getattr(f, "changes", 0),
                        "patch": getattr(f, "patch", None),
                        "blob_url": getattr(f, "blob_url", None),
                        "raw_url": getattr(f, "raw_url", None),
                        "previous_filename": getattr(f, "previous_filename", None),
                    })
    except Exception: return [], []
    return files, file_stats

def get_pr_full_stats(api: GhApi, owner: str, repo: str, prn: int) -> Dict:
    stats = {"total_additions": 0, "total_deletions": 0, "changed_files": 0, "commits": 0, "review_comments": 0, "comments": 0}
    try:
        pr_detail = api.pulls.get(owner, repo, prn)
        stats["total_additions"] = getattr(pr_detail, "additions", 0)
        stats["total_deletions"] = getattr(pr_detail, "deletions", 0)
        stats["changed_files"] = getattr(pr_detail, "changed_files", 0)
        stats["commits"] = getattr(pr_detail, "commits", 0)
        stats["review_comments"] = getattr(pr_detail, "review_comments", 0)
        stats["comments"] = getattr(pr_detail, "comments", 0)
    except Exception: pass
    return stats

def get_file_content_safe(api: GhApi, owner: str, repo: str, path: str, ref: str) -> Optional[str]:
    try:
        obj = api.repos.get_content(owner, repo, path, ref=ref)
        content = getattr(obj, "content", None)
        if content: return base64.b64decode(content).decode("utf-8", errors="ignore")
    except Exception: pass
    return None

def process_single_repo(repo_meta: dict, token: str, fout, args):
    """Processes a single repository specifically looking for PRs using multi-threading for API calls"""
    owner, repo_name = repo_meta['full_name'].split("/", 1)
    print(f"   [Worker] Target Locked: {repo_meta['full_name']} (Using token endpoint)", file=sys.stderr)
    lic = repo_meta['license']
    stars = repo_meta['stars']
    # thread-local GhApi instance
    api = GhApi(token=token)
    
    tasks = []
    try:
        for page in paged(api.pulls.list, owner, repo_name, state="closed", sort="updated", direction="desc", per_page=100):
            for pr in page or []:
                if len(tasks) >= args.max_tasks_per_repo:
                    break
                    
                if not getattr(pr, "merged_at", None):
                    continue
                    
                pr_num = pr.number
                base_sha = getattr(getattr(pr, "base", None), "sha", None)
                head_sha = getattr(getattr(pr, "head", None), "sha", None)
                if not base_sha or not head_sha: continue
                
                changed, file_stats = get_changed_verilog_files_with_stats(api, owner, repo_name, pr_num)
                if not changed: continue
                
                # We have changes, now grab full stats
                pr_stats = get_pr_full_stats(api, owner, repo_name, pr_num)
                
                context_files, target_files = {}, {}
                for p in changed:
                    pre = get_file_content_safe(api, owner, repo_name, p, base_sha)
                    post = get_file_content_safe(api, owner, repo_name, p, head_sha)
                    if pre is not None: context_files[p] = pre
                    if post is not None: target_files[p] = post
                
                if not context_files: continue
                
                # Minimal comment fetching to avoid API burst
                comments = []
                # (Optional patch fetching inside here to save limit could be added, but skipping to save quota)
                
                text = f"{pr.title}\n{pr.body or ''}\n"
                if re.search(r"\b(bug|fix|regression)\b", text, re.I): task_type = "bug-fix"
                elif re.search(r"\b(remove|deprecat(e|ion|ed))\b", text, re.I): task_type = "feature-removal"
                elif re.search(r"\b(feature|enhance|add|implement)\b", text, re.I): task_type = "feature-addition"
                else: task_type = "other"
                
                dp = {
                    "repo": repo_meta['full_name'],
                    "stars": stars,
                    "license": lic,
                    "pull_request": {
                        "number": pr_num, "title": pr.title, "body": pr.body,
                        "merged_at": getattr(pr, "merged_at", None), "html_url": getattr(pr, "html_url", None),
                        "base_sha": base_sha, "head_sha": head_sha, "stats": pr_stats,
                    },
                    "file_stats": file_stats,
                    "context_files": context_files,
                    "target_files": target_files,
                    "diff": "",
                    "comments": comments,
                    "task_type": task_type,
                }
                tasks.append(dp)
                
                # Stream write
                if fout and not args.dry_run:
                    with write_lock:
                        fout.write(json.dumps(dp, ensure_ascii=False) + "\n")
                        fout.flush()
                        
            if len(tasks) >= args.max_tasks_per_repo:
                break
    except Exception as e:
        log(f"[repo error] {repo_meta['full_name']}: {e}", args.debug)
        
    return repo_meta['full_name'], len(tasks)


def main():
    p = argparse.ArgumentParser(description="FAST PR Dataset Builder (Globally Sorted by Stars)")
    p.add_argument("--output", required=True, help="Output JSONL file")
    p.add_argument("--input", required=False, help="Text file with repo names (one per line or ndjson)")
    p.add_argument("--max_tasks_per_repo", type=int, default=50, help="Max PRs/tasks per repo")
    p.add_argument("--debug", action="store_true", help="Verbose log")
    p.add_argument("--dry-run", action="store_true", help="Do not write output; just log")
    p.add_argument("--workers", type=int, default=10, help="Number of concurrent repository processor threads")
    p.add_argument("--total-nodes", type=int, default=1, help="Total number of distributed systems/PCs you are running this on")
    p.add_argument("--node-index", type=int, default=1, help="The ID of this specific PC (e.g., 1, 2, or 3)")
    args = p.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    tokens_str = os.getenv("GITHUB_TOKENS") or os.getenv("GITHUB_TOKEN")
    if not tokens_str:
        raise RuntimeError("GITHUB_TOKENS not set in .env!")
        
    token_list = [t.strip() for t in tokens_str.split(",") if t.strip()]
    api = GhApi(token=token_list[0])
    already_collected = load_already_collected(args.output)
    print(f"Skipping {len(already_collected)} already collected repos.")

    print("Checking repository source...", file=sys.stderr)
    repos_metadata = []
    if args.input and os.path.exists(args.input):
        with open(args.input, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                if line.startswith('{'):
                    try:
                        obj = json.loads(line)
                        r = obj.get("repo") or obj.get("full_name")
                        if r: 
                            repos_metadata.append({'full_name': r, 'stars': obj.get("stargazers_count", 0), 'license': {"spdx_id": "", "text": ""}})
                    except Exception: pass
                elif "/" in line:
                    repos_metadata.append({'full_name': line, 'stars': 0, 'license': {"spdx_id": "", "text": ""}})
        # unique
        seen = set()
        unique_repos = []
        for r in repos_metadata:
            if r['full_name'] not in seen:
                seen.add(r['full_name'])
                unique_repos.append(r)
        repos_metadata = unique_repos
    else:
        print("Searching repositories with 4-Token high velocity multi-cycling...", file=sys.stderr)
        import urllib.parse
        repos_metadata = fast_search_repositories_with_stars(api, args.debug, token_list)
        
    # SORT by Stars Descending
    repos_metadata.sort(key=lambda x: x.get('stars', 0), reverse=True)
    
    pending_repos = [r for r in repos_metadata if r['full_name'] not in already_collected]

    if args.total_nodes > 1:
        if args.node_index < 1 or args.node_index > args.total_nodes:
            raise ValueError("--node-index must be between 1 and --total-nodes")
        # Sub-partition the pending repos natively using modulus math so no repo is repeated across PCs.
        node_repos = []
        for i, r in enumerate(pending_repos):
            if (i % args.total_nodes) == (args.node_index - 1):
                node_repos.append(r)
        pending_repos = node_repos
        print(f"Distributed Chunking: This PC (Node {args.node_index}/{args.total_nodes}) was allocated its strictly exclusive {len(pending_repos)} repos.", file=sys.stderr)

    if not pending_repos:
        print(f"All {len(repos_metadata)} repos have already been processed! Nothing to do.", file=sys.stderr)
        return
        
    print(f"Found {len(repos_metadata)} total repos. Processing {len(pending_repos)} pending repos globally sorted by stars (Highest: {pending_repos[0].get('stars', 0)} stars).", file=sys.stderr)

    out_path = Path(args.output)
    fout = None if args.dry_run else out_path.open("a", encoding="utf-8")

    total_tasks = 0
    scraped_repo_count = 0
    start_time = time.time()
    
    # 2. Process repos concurrently using ThreadPoolExecutor
    print(f"Starting {args.workers} workers to process repos for PRs...", file=sys.stderr)
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        import random
        future_to_repo = {executor.submit(process_single_repo, r, random.choice(token_list), fout, args): r for r in pending_repos}
        
        for future in as_completed(future_to_repo):
            try:
                full_name, tasks_found = future.result()
                total_tasks += tasks_found
                scraped_repo_count += 1
                
                elapsed = time.time() - start_time
                rate = scraped_repo_count / elapsed * 3600
                print(f"[{scraped_repo_count}/{len(pending_repos)}] {full_name} processed ({tasks_found} PRs). Est. rate: {rate:.0f} repos/hr", file=sys.stderr)
                
            except Exception as e:
                repo_meta = future_to_repo[future]
                print(f"[Error] Failed to process {repo_meta['full_name']}: {e}", file=sys.stderr)

    if fout:
        fout.close()
    
    elapsed = time.time() - start_time
    print(f"\nDone. Processed {scraped_repo_count} repos, extracted {total_tasks} PR datapoints in {elapsed:.1f}s", file=sys.stderr)

if __name__ == "__main__":
    main()
