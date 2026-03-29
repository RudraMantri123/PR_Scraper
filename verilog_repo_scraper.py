import argparse, json, os, sys, time
import urllib.request
import urllib.parse
from pathlib import Path

from ghapi.all import GhApi
from verilog_dataset_builder import search_repositories, split_repo_full_name, get_repo_license

VERILOG_EXT = (".v", ".sv", ".vh", ".svh")
UNLIMITED = 999_999_999

def log(msg, enabled):
    if enabled:
        print(msg, file=sys.stderr)

def get_default_branch(api, owner, repo):
    try:
        r = api.repos.get(owner, repo)
        return getattr(r, "default_branch", "main")
    except Exception as e:
        print(f"[warn] {owner}/{repo}: Could not get default branch: {e}", file=sys.stderr)
        return "main"

def get_verilog_files_from_tree(api, owner, repo, branch, debug):
    try:
        tree = api.git.get_tree(owner, repo, branch, recursive='1')
        items = getattr(tree, "tree", [])
        verilog_files = []
        for item in items:
            path = getattr(item, "path", "")
            if getattr(item, "type", "") == "blob" and path.lower().endswith(VERILOG_EXT):
                verilog_files.append(path)
        return verilog_files
    except Exception as e:
        print(f"[warn] {owner}/{repo}: Could not fetch git tree for branch '{branch}': {e}", file=sys.stderr)
        return []

def download_raw_file(owner, repo, branch, path, token):
    # Using urllib.parse.quote for path components to handle spaces/special characters
    safe_branch = urllib.parse.quote(branch)
    safe_path = urllib.parse.quote(path)
    url = f"https://raw.githubusercontent.com/{owner}/{repo}/{safe_branch}/{safe_path}"
    
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"token {token}")
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        return None

def load_scraped(filename):
    if not os.path.exists(filename):
        return set()
    with open(filename, "r", encoding="utf-8") as f:
         return set(line.strip() for line in f if line.strip())

def mark_scraped(repo_full_name, filename):
    with open(filename, "a", encoding="utf-8") as f:
        f.write(repo_full_name + "\n")
        f.flush()

def main():
    p = argparse.ArgumentParser(description="Scrape entire Verilog repositories")
    p.add_argument("--output", required=True, help="Output NDJSON file")
    p.add_argument("--tracker", default="scraped_repos.txt", help="File to track completed repos")
    p.add_argument("--max_repos", type=int, default=25, help="Max repos to collect (ignored with --unlimited)")
    p.add_argument("--unlimited", action="store_true", help="Remove caps — scrape every repo found")
    p.add_argument("--max_files_per_repo", type=int, default=500, help="Max files to download per repo to prevent massive spikes")
    p.add_argument("--debug", action="store_true", help="Verbose log")
    p.add_argument("--dry-run", action="store_true", help="Do not write output or tracker; just log")
    args = p.parse_args()

    if args.unlimited:
        args.max_repos = UNLIMITED

    from dotenv import load_dotenv
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN not set in environment or .env file")

    api = GhApi(token=token)

    already_collected = load_scraped(args.tracker)
    if already_collected:
        print(f"[resume] Loaded {len(already_collected)} already-collected repos from {args.tracker}", file=sys.stderr)

    repos = search_repositories(api, max_repos=args.max_repos, debug=args.debug)
    if not repos:
        print("No repositories matched search criteria.", file=sys.stderr)
        return

    out_path = Path(args.output)
    fout = None if args.dry_run else out_path.open("a", encoding="utf-8")

    total_files = 0
    scraped_repo_count = 0

    for full_name in repos:
        if scraped_repo_count >= args.max_repos:
            break

        if full_name in already_collected:
            log(f"[skip] {full_name}: already in {args.tracker}", args.debug)
            continue

        owner, repo = split_repo_full_name(full_name)
        lic = get_repo_license(api, owner, repo, args.debug)
        if not lic:
            log(f"[skip] {full_name}: non-permissive or missing license", args.debug)
            if not args.dry_run:
                mark_scraped(full_name, args.tracker)
            continue

        branch = get_default_branch(api, owner, repo)
        files = get_verilog_files_from_tree(api, owner, repo, branch, args.debug)
        
        if not files:
            log(f"[repo] {full_name}: No Verilog files found in tree", args.debug)
            if not args.dry_run:
                mark_scraped(full_name, args.tracker)
            continue

        if len(files) > args.max_files_per_repo:
            log(f"[cap] {full_name}: trimming {len(files)} files to {args.max_files_per_repo}", args.debug)
            files = files[:args.max_files_per_repo]

        repo_file_count = 0
        for path in files:
            content = download_raw_file(owner, repo, branch, path, token)
            if content is not None:
                dp = {
                    "repo": full_name,
                    "license": lic,
                    "branch": branch,
                    "file_path": path,
                    "content": content
                }
                if not args.dry_run:
                    fout.write(json.dumps(dp, ensure_ascii=False) + "\n")
                    fout.flush()
                repo_file_count += 1
            time.sleep(0.05)
        
        print(f"[repo] {full_name}: downloaded {repo_file_count} files", file=sys.stderr)
        total_files += repo_file_count
        scraped_repo_count += 1

        if not args.dry_run:
            mark_scraped(full_name, args.tracker)

    if fout:
        fout.close()
    
    print(f"\nDone. Processed {scraped_repo_count} new repos, wrote {total_files} file datapoints to {args.output}", file=sys.stderr)

if __name__ == "__main__":
    main()
