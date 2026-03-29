import argparse, json, os, sys, time
import urllib.request
import urllib.parse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from ghapi.all import GhApi, paged
from verilog_dataset_builder import PERMISSIVE

VERILOG_EXT = (".v", ".sv", ".vh", ".svh")
UNLIMITED = 999_999_999

def log(msg, enabled=True):
    if enabled:
        print(msg, file=sys.stderr)

# We use the same queries as verilog_dataset_builder.py
queries = [
    "language:Verilog stars:>500", "language:Verilog stars:100..500", "language:Verilog stars:50..100",
    "language:Verilog stars:20..50", "language:Verilog stars:10..20", "language:Verilog stars:5..10",
    "language:Verilog stars:2..5", "language:Verilog stars:1..2",
    "language:SystemVerilog stars:>500", "language:SystemVerilog stars:100..500", "language:SystemVerilog stars:50..100",
    "language:SystemVerilog stars:20..50", "language:SystemVerilog stars:10..20", "language:SystemVerilog stars:5..10",
    "language:SystemVerilog stars:2..5", "language:SystemVerilog stars:1..2",
    "topic:verilog stars:>1", "topic:systemverilog stars:>1",
    "topic:fpga language:Verilog stars:>1", "topic:risc-v language:Verilog stars:>1",
    "topic:riscv language:Verilog stars:>1", "topic:rtl language:Verilog stars:>1",
    "topic:hardware language:Verilog stars:>1", "topic:asic language:Verilog stars:>1",
    "topic:hdl language:Verilog stars:>1",
    "topic:fpga language:SystemVerilog stars:>1", "topic:risc-v language:SystemVerilog stars:>1",
    "topic:asic language:SystemVerilog stars:>1", "topic:rtl language:SystemVerilog stars:>1",
    "topic:hdl language:SystemVerilog stars:>1",
    "verilog cpu stars:>5 language:Verilog", "verilog uart stars:>5 language:Verilog",
    "verilog spi stars:>5 language:Verilog", "verilog i2c stars:>5 language:Verilog",
    "verilog axi stars:>5 language:Verilog", "verilog ddr stars:>5 language:Verilog",
    "verilog ethernet stars:>5 language:Verilog", "verilog dma stars:>5 language:Verilog",
    "verilog fifo stars:>5 language:Verilog", "verilog pcie stars:>5 language:Verilog",
    "verilog usb stars:>5 language:Verilog", "verilog sha stars:>5 language:Verilog",
    "verilog aes stars:>5 language:Verilog", "verilog fpu stars:>5 language:Verilog",
    "verilog mips stars:>5 language:Verilog", "verilog riscv stars:>5 language:Verilog",
    "verilog hdmi stars:>5 language:Verilog", "verilog vga stars:>5 language:Verilog",
    "verilog sdram stars:>5 language:Verilog", "verilog pwm stars:>5 language:Verilog",
    "verilog cache stars:>5 language:Verilog", "verilog arbiter stars:>5 language:Verilog",
    "verilog pipeline stars:>5 language:Verilog", "verilog timer stars:>5 language:Verilog",
    "verilog wishbone stars:>5 language:Verilog", "verilog avalon stars:>5 language:Verilog",
    "verilog apb stars:>5 language:Verilog", "verilog ahb stars:>5 language:Verilog",
    "verilog fft stars:>5 language:Verilog", "verilog cordic stars:>5 language:Verilog",
    "verilog crc stars:>5 language:Verilog", "verilog noc stars:>5 language:Verilog",
    "systemverilog cpu stars:>5 language:SystemVerilog", "systemverilog uart stars:>5 language:SystemVerilog",
    "systemverilog axi stars:>5 language:SystemVerilog", "systemverilog fifo stars:>5 language:SystemVerilog",
    "systemverilog cache stars:>5 language:SystemVerilog", "systemverilog dma stars:>5 language:SystemVerilog",
    "systemverilog pipeline stars:>5 language:SystemVerilog", "systemverilog pcie stars:>5 language:SystemVerilog",
    "systemverilog usb stars:>5 language:SystemVerilog",
    "topic:opencores language:Verilog", "topic:open-source-hardware language:Verilog",
    "topic:tinytapeout language:Verilog", "topic:openlane language:Verilog",
    "topic:skywater language:Verilog", "topic:caravel language:Verilog",
    "topic:ice40 language:Verilog", "topic:ecp5 language:Verilog",
    "topic:xilinx language:Verilog", "topic:altera language:Verilog",
    "topic:lattice language:Verilog", "topic:gowin language:Verilog",
    "topic:zynq language:Verilog",
    "language:Verilog created:>2023-01-01 stars:>2", "language:Verilog created:2021-01-01..2023-01-01 stars:>2",
    "language:Verilog created:2019-01-01..2021-01-01 stars:>2", "language:Verilog created:2017-01-01..2019-01-01 stars:>2",
    "language:Verilog created:2015-01-01..2017-01-01 stars:>2", "language:Verilog created:<2015-01-01 stars:>2",
    "language:SystemVerilog created:>2023-01-01 stars:>2", "language:SystemVerilog created:2021-01-01..2023-01-01 stars:>2",
    "language:SystemVerilog created:2019-01-01..2021-01-01 stars:>2", "language:SystemVerilog created:<2019-01-01 stars:>2",
    "language:Verilog pushed:>2024-06-01 stars:>1", "language:Verilog pushed:2023-01-01..2024-06-01 stars:>1",
    "language:Verilog pushed:2021-01-01..2023-01-01 stars:>1",
    "language:SystemVerilog pushed:>2024-06-01 stars:>1", "language:SystemVerilog pushed:2023-01-01..2024-06-01 stars:>1",
    "language:Verilog size:>50000 stars:>1", "language:Verilog size:10000..50000 stars:>1",
    "language:Verilog size:5000..10000 stars:>1", "language:Verilog size:1000..5000 stars:>1",
    "language:SystemVerilog size:>50000 stars:>1", "language:SystemVerilog size:10000..50000 stars:>1",
    "language:SystemVerilog size:1000..10000 stars:>1",
    "language:Verilog forks:>50 stars:>5", "language:Verilog forks:10..50 stars:>5",
    "language:SystemVerilog forks:>50 stars:>5", "language:SystemVerilog forks:10..50 stars:>5",
]

# Thread safety utilities
write_lock = threading.Lock()
# Rate limiting tracking module variables
rate_limit_reset = 0
rate_limit_lock = threading.Lock()

def check_rate_limit(headers):
    global rate_limit_reset
    if 'X-RateLimit-Remaining' in headers:
        rem = int(headers['X-RateLimit-Remaining'])
        if rem <= 5:  # Sleep a bit early to avoid 403
            reset_time = int(headers.get('X-RateLimit-Reset', time.time() + 60))
            with rate_limit_lock:
                if reset_time > rate_limit_reset:
                    rate_limit_reset = reset_time
                    wait_time = max(0, rate_limit_reset - time.time()) + 1
                    log(f"Rate limit almost exhausted. Sleeping for {wait_time:.1f} seconds until reset...")
                    time.sleep(wait_time)

def handle_rate_limit_error(e):
    global rate_limit_reset
    if hasattr(e, "headers") and 'X-RateLimit-Reset' in e.headers:
        reset_time = int(e.headers['X-RateLimit-Reset'])
        with rate_limit_lock:
            if reset_time > rate_limit_reset:
                rate_limit_reset = reset_time
                wait_time = max(0, rate_limit_reset - time.time()) + 1
                log(f"Rate limit fully exhausted! Sleeping for {wait_time:.1f} seconds...")
                time.sleep(wait_time)
        return True
    return False

def fast_search_repositories(api, max_repos, debug, start_delay=0.0):
    seen = set()
    repos_metadata = [] # stores dictionaries
    try:
        for q in queries:
            log(f"[search] query: {q}", debug)
            try:
                for page in paged(api.search.repos, q=q, sort="stars", order="desc", per_page=100):
                    items = page.get("items", []) if isinstance(page, dict) else getattr(page, "items", []) or []
                    for it in items:
                        full_name = it.get("full_name") if isinstance(it, dict) else getattr(it, "full_name", None)
                        if not full_name or full_name in seen:
                            continue
                        lic = it.get("license") if isinstance(it, dict) else getattr(it, "license", None)
                        spdx = ((lic or {}).get("spdx_id", "") if isinstance(lic, dict) else getattr(lic, "spdx_id", "")).lower()
                        if spdx in PERMISSIVE:
                            seen.add(full_name)
                            branch = it.get("default_branch") if isinstance(it, dict) else getattr(it, "default_branch", "main")
                            
                            # Construct license dict matching verilog_dataset_builder.py
                            license_dict = {"spdx_id": (lic or {}).get("spdx_id", "") if isinstance(lic, dict) else getattr(lic, "spdx_id", ""), "text": ""}
                            
                            repos_metadata.append({
                                'full_name': full_name,
                                'branch': branch,
                                'license': license_dict,
                            })
                            
                            if len(repos_metadata) >= max_repos:
                                return repos_metadata
                    time.sleep(0.1)
            except Exception as e:
                log(f"[warn] query '{q}' hit limit or error: {e}", debug)
                if hasattr(e, "code") and e.code == 403:
                    time.sleep(60) # if we hit rate limit on search, wait a minute
                continue
    except Exception as e:
        log(f"Search aborted. Exception: {e}", debug)
    return repos_metadata

def fetch_tree_files(owner, repo, branch, token, max_retries=3):
    url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{urllib.parse.quote(branch)}?recursive=1"
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"token {token}")
    
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                check_rate_limit(response.headers)
                data = json.loads(response.read().decode('utf-8'))
                files = []
                for item in data.get("tree", []):
                    path = item.get("path", "")
                    if item.get("type") == "blob" and path.lower().endswith(VERILOG_EXT):
                        files.append(path)
                return files
        except urllib.error.HTTPError as e:
            if handle_rate_limit_error(e):
                continue
            # For 404s or other errors, stop trying
            break
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                break
    return []

def download_raw_file(owner, repo, branch, path, token, max_retries=3):
    safe_branch = urllib.parse.quote(branch)
    safe_path = urllib.parse.quote(path)
    # Note: raw.githubusercontent.com is not API rate-limited in the same way, but token can be used
    url = f"https://raw.githubusercontent.com/{owner}/{repo}/{safe_branch}/{safe_path}"
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"token {token}")
        
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.read().decode('utf-8', errors='ignore')
        except urllib.error.HTTPError as e:
            # wait briefly if rate limited
            if e.code == 429:
                time.sleep(3 * (attempt + 1))
            elif e.code == 404:
                return None
            else:
                time.sleep(1)
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
    return None

def process_repo(repo_meta, token, fout, args):
    full_name = repo_meta['full_name']
    owner, repo_name = full_name.split("/", 1)
    branch = repo_meta['branch']
    lic = repo_meta['license']
    
    files = fetch_tree_files(owner, repo_name, branch, token)
    if not files:
        log(f"[repo] {full_name}: No Verilog files found in tree or error fetching", args.debug)
        return full_name, 0
        
    if len(files) > args.max_files_per_repo:
        log(f"[cap] {full_name}: trimming {len(files)} files to {args.max_files_per_repo}", args.debug)
        files = files[:args.max_files_per_repo]
        
    # Parallelize file downloads with a pool local to this repo, or do them sequentially fast
    # Creating a ThreadPoolExecutor per repo allows concurrent file fetching
    repo_file_count = 0
    with ThreadPoolExecutor(max_workers=min(20, len(files))) as file_executor:
        future_to_path = {file_executor.submit(download_raw_file, owner, repo_name, branch, path, token): path for path in files}
        
        for future in as_completed(future_to_path):
            path = future_to_path[future]
            content = future.result()
            if content is not None:
                dp = {
                    "repo": full_name,
                    "license": lic,
                    "branch": branch,
                    "file_path": path,
                    "content": content
                }
                if not args.dry_run and fout:
                    # Thread-safe write
                    with write_lock:
                        fout.write(json.dumps(dp, ensure_ascii=False) + "\n")
                        fout.flush()
                repo_file_count += 1

    return full_name, repo_file_count

def load_scraped(filename):
    if not os.path.exists(filename):
        return set()
    with open(filename, "r", encoding="utf-8") as f:
         return set(line.strip() for line in f if line.strip())

def mark_scraped(repo_full_name, filename):
    with write_lock:
        with open(filename, "a", encoding="utf-8") as f:
            f.write(repo_full_name + "\n")
            f.flush()

def main():
    p = argparse.ArgumentParser(description="FAST Scrape entire Verilog repositories (Multithreaded)")
    p.add_argument("--output", required=True, help="Output NDJSON file")
    p.add_argument("--tracker", default="scraped_repos.txt", help="File to track completed repos")
    p.add_argument("--max_repos", type=int, default=25, help="Max repos to collect (ignored with --unlimited)")
    p.add_argument("--unlimited", action="store_true", help="Remove caps — scrape every repo found")
    p.add_argument("--max_files_per_repo", type=int, default=500, help="Max files to download per repo to prevent massive spikes")
    p.add_argument("--debug", action="store_true", help="Verbose log")
    p.add_argument("--dry-run", action="store_true", help="Do not write output or tracker; just log")
    p.add_argument("--workers", type=int, default=50, help="Number of concurrent repository scraper threads")
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

    # 1. Gather all repos fast
    print("Searching repositories (this is fast as metadata is harvested directly from search)...", file=sys.stderr)
    all_repos_metadata = fast_search_repositories(api, max_repos=args.max_repos + len(already_collected), debug=args.debug)
    
    if not all_repos_metadata:
        print("No repositories matched search criteria.", file=sys.stderr)
        return

    # Filter already collected
    pending_repos = [r for r in all_repos_metadata if r['full_name'] not in already_collected]
    
    # Trim to max_repos
    if not args.unlimited:
        pending_repos = pending_repos[:args.max_repos]
        
    print(f"Found {len(all_repos_metadata)} repos, {len(pending_repos)} are pending and will be scraped.", file=sys.stderr)

    out_path = Path(args.output)
    fout = None if args.dry_run else out_path.open("a", encoding="utf-8")

    total_files = 0
    scraped_repo_count = 0

    # 2. Process repos concurrently using ThreadPoolExecutor
    print(f"Starting {args.workers} workers to process repositories...", file=sys.stderr)
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_repo = {executor.submit(process_repo, r, token, fout, args): r for r in pending_repos}
        
        for i, future in enumerate(as_completed(future_to_repo)):
            try:
                full_name, repo_file_count = future.result()
                total_files += repo_file_count
                scraped_repo_count += 1
                
                if not args.dry_run:
                    mark_scraped(full_name, args.tracker)
                    
                elapsed = time.time() - start_time
                rate = scraped_repo_count / elapsed * 3600 # repos per hour
                print(f"[{scraped_repo_count}/{len(pending_repos)}] {full_name} done ({repo_file_count} files). Est. rate: {rate:.0f} repos/hr", file=sys.stderr)
                
            except Exception as e:
                repo_meta = future_to_repo[future]
                print(f"[Error] Failed to process {repo_meta['full_name']}: {e}", file=sys.stderr)

    if fout:
        fout.close()
    
    elapsed = time.time() - start_time
    print(f"\nDone. Processed {scraped_repo_count} new repos, wrote {total_files} file datapoints to {args.output} in {elapsed:.1f}s", file=sys.stderr)

if __name__ == "__main__":
    main()
