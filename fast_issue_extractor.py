import argparse, json, os, sys, time, urllib.request, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

VERILOG_EXT = (".v", ".sv", ".vh", ".svh")
UNLIMITED = 999_999_999

write_lock = threading.Lock()

def log(msg: str, enabled: bool = True):
    if enabled:
        print(msg, file=sys.stderr)

class TokenManager:
    def __init__(self, token_list):
        self.tokens = token_list
        self.lock = threading.Lock()
        self.exhausted_until = {t: 0 for t in token_list}
        self.index = 0

    def get_token(self):
        with self.lock:
            now = time.time()
            available = [t for t in self.tokens if self.exhausted_until[t] <= now]
            if not available:
                nearest_reset = min(self.exhausted_until.values())
                wait_time = max(0, nearest_reset - now) + 5
                log(f"\n[RATE LIMIT] ALL {len(self.tokens)} TOKENS EXHAUSTED! Sleeping for {wait_time:.1f} seconds (\u2248{wait_time/60:.1f} minutes)...")
                time.sleep(wait_time)
                # After sleeping, min exhausted should be available
                return min(self.exhausted_until, key=self.exhausted_until.get)
            
            # Simple round-robin over available
            token = available[self.index % len(available)]
            self.index += 1
            return token

    def mark_exhausted(self, token, reset_time):
        with self.lock:
            if reset_time > self.exhausted_until[token]:
                self.exhausted_until[token] = reset_time
                log(f"\n[TOKEN LIMIT] A token was depleted. Rotating to next token. (Resumes at {reset_time})")

def make_api_request(url, token_manager, debug=False, max_retries=5):
    """Makes an API request and cleanly rotates tokens on limit."""
    for attempt in range(max_retries):
        token = token_manager.get_token()
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"token {token}")
        req.add_header("Accept", "application/vnd.github.v3+json")

        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                headers = response.headers
                if 'X-RateLimit-Remaining' in headers:
                    rem = int(headers['X-RateLimit-Remaining'])
                    if rem <= 2: 
                        reset_time = int(headers.get('X-RateLimit-Reset', time.time() + 60))
                        token_manager.mark_exhausted(token, reset_time)
                
                return json.loads(response.read().decode('utf-8')), headers
        except urllib.error.HTTPError as e:
            if e.code == 403 and getattr(e, "headers", None) and 'X-RateLimit-Remaining' in e.headers:
                rem = int(e.headers.get('X-RateLimit-Remaining', 1))
                if rem == 0:
                    reset_time = int(e.headers.get('X-RateLimit-Reset', time.time() + 60))
                    token_manager.mark_exhausted(token, reset_time)
                    continue # Retry instantly with a shiny new token!
            if e.code == 404:
                return None, None
            if getattr(e, "code", 500) >= 500:
                time.sleep(2 ** attempt)
                continue
            return None, None
        except Exception:
            time.sleep(2 ** attempt)
            continue
    return None, None

def fetch_all_issues(owner, repo, token_manager, debug):
    all_issues = []
    page = 1
    
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/issues?state=all&per_page=100&page={page}"
        data, headers = make_api_request(url, token_manager, debug=debug)
        
        if data is None or not isinstance(data, list):
            break
        if len(data) == 0:
            break
            
        for item in data:
            if "pull_request" not in item:
                all_issues.append({
                    "number": item.get("number"),
                    "title": item.get("title"),
                    "state": item.get("state"),
                    "body": item.get("body"),
                    "created_at": item.get("created_at"),
                    "closed_at": item.get("closed_at"),
                    "labels": [lbl.get("name", "").lower() for lbl in item.get("labels", []) if isinstance(lbl, dict)]
                })
        
        link_header = headers.get('Link', '') if headers else ''
        if 'rel="next"' not in link_header:
            break
            
        page += 1
        time.sleep(0.02)
        
    return all_issues

def process_repo(repo_full_name, token_manager, fout, args):
    try:
        owner, repo_str = repo_full_name.split("/", 1)
        issues = fetch_all_issues(owner, repo_str, token_manager, args.debug)
        
        if not issues:
            return repo_full_name, 0
            
        dp = {
            "repo": repo_full_name,
            "issues": issues,
            "total_issues": len(issues)
        }
        
        if not args.dry_run and fout:
            with write_lock:
                fout.write(json.dumps(dp, ensure_ascii=False) + "\n")
                fout.flush()
                
        return repo_full_name, len(issues)
        
    except Exception as e:
        log(f"[Error parsing {repo_full_name}]: {e}", args.debug)
        return repo_full_name, 0

def load_already_collected(output_path):
    collected = set()
    p = Path(output_path)
    if not p.exists(): return collected
    with open(output_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rec = json.loads(line)
                    if rec.get("repo"):
                        collected.add(rec["repo"])
                except Exception: pass
    return collected

def main():
    p = argparse.ArgumentParser(description="Endless 80k Repo Issue Extractor (Multi-token Array)")
    p.add_argument("--input", required=False, help="Text file with repo names")
    p.add_argument("--output", required=True, help="NDJSON Output file for the issues")
    p.add_argument("--workers", type=int, default=15, help="Concurrent workers")
    p.add_argument("--debug", action="store_true", help="Verbose log")
    p.add_argument("--dry-run", action="store_true", help="Do not write output; just log")
    args = p.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    
    # We load GITHUB_TOKENS (comma separated) or fallback to GITHUB_TOKEN
    tokens_str = os.getenv("GITHUB_TOKENS") or os.getenv("GITHUB_TOKEN")
    if not tokens_str:
        raise RuntimeError("GITHUB_TOKENS not set in .env!")
        
    token_list = [t.strip() for t in tokens_str.split(",") if t.strip()]
    print(f"Loaded {len(token_list)} tokens for rotation.", file=sys.stderr)
    
    token_manager = TokenManager(token_list)

    repos = []
    if args.input:
        if not os.path.exists(args.input):
            print(f"Error: Could not find '{args.input}'", file=sys.stderr)
            return
        with open(args.input, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                if line.startswith('{'):
                    try:
                        obj = json.loads(line)
                        r = obj.get("repo") or obj.get("full_name")
                        if r: repos.append(r)
                    except Exception: pass
                elif "/" in line:
                    repos.append(line)
        repos = list(dict.fromkeys(repos))
    else:
        from ghapi.all import GhApi
        from fast_dataset_builder import fast_search_repositories_with_stars
        api = GhApi(token=token_list[0]) # use first token for initial search
        print("No input file provided. Automatically searching GitHub for ALL repositories...", file=sys.stderr)
        found_repos_meta = fast_search_repositories_with_stars(api, args.debug)
        repos = [r['full_name'] for r in found_repos_meta]

    already_collected = load_already_collected(args.output)
    pending_repos = sorted([r for r in repos if r not in already_collected])
    
    print(f"Total Repos Found: {len(repos)}", file=sys.stderr)
    print(f"Already Scraped: {len(already_collected)}", file=sys.stderr)
    print(f"Pending Scrape: {len(pending_repos)}", file=sys.stderr)

    out_path = Path(args.output)
    fout = None if args.dry_run else out_path.open("a", encoding="utf-8")

    total_issues = 0
    scraped_repo_count = 0
    start_time = time.time()
    
    print(f"Starting {args.workers} sweeping workers scaling horizontally...", file=sys.stderr)
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_repo = {executor.submit(process_repo, r, token_manager, fout, args): r for r in pending_repos}
        
        for future in as_completed(future_to_repo):
            full_name = future_to_repo[future]
            try:
                _, issues_found = future.result()
                total_issues += issues_found
                scraped_repo_count += 1
                
                elapsed = time.time() - start_time
                rate = scraped_repo_count / elapsed * 3600
                print(f"[{scraped_repo_count}/{len(pending_repos)}] {full_name} processed ({issues_found} issues). Est. repo/hr: {rate:.0f}", file=sys.stderr)
            except Exception as e:
                print(f"[Fatal Error] {full_name}: {e}", file=sys.stderr)

    if fout:
        fout.close()
    
    elapsed = time.time() - start_time
    print(f"\nDone. Processed {scraped_repo_count} repos, extracted {total_issues} issue datapoints in {elapsed:.1f}s", file=sys.stderr)

if __name__ == "__main__":
    main()
