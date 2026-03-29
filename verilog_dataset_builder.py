import argparse, base64, json, os, re, time, sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set

from ghapi.all import GhApi, paged

# ====== Config ======
PERMISSIVE = {
    "mit", "bsd-2-clause", "bsd-3-clause", "bsd-2-clause-clear", "isc",
    "apache-2.0", "lgpl-2.1", "lgpl-3.0", "mpl-2.0", "unlicense"
}
VERILOG_EXT = (".v", ".sv", ".vh", ".svh")
CLOSE_RE = re.compile(
    r"(?:close[sd]?|fixe?[sd]?|resolve[sd]?)[\s:]*#\s*(\d+)\b", re.I
)
UNLIMITED = 999_999_999  # sentinel for "no cap"

def log(msg: str, enabled: bool):
    if enabled:
        print(msg, file=sys.stderr)

def is_verilog_file(path: str) -> bool:
    return path.lower().endswith(VERILOG_EXT)

def split_repo_full_name(full_name: str) -> Tuple[str, str]:
    owner, repo = full_name.split("/", 1)
    return owner, repo

def load_already_collected(output_path: str) -> Set[str]:
    """Read existing NDJSON and return set of already collected repo names."""
    collected = set()
    p = Path(output_path)
    if not p.exists():
        return collected
    with open(output_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rec = json.loads(line)
                    repo = rec.get("repo", "")
                    if repo:
                        collected.add(repo)
                except Exception:
                    continue
    return collected

def search_repositories(api: GhApi, max_repos: int, debug: bool) -> List[str]:
    """
    Search across multiple query buckets to maximize unique repo discovery.
    Each bucket targets a different star range or topic to stay under GitHub's
    1000-result-per-query hard limit.
    """
    queries = [
        # Language: Verilog — split by star range
        "language:Verilog stars:>500",
        "language:Verilog stars:100..500",
        "language:Verilog stars:50..100",
        "language:Verilog stars:20..50",
        "language:Verilog stars:10..20",
        "language:Verilog stars:5..10",
        "language:Verilog stars:2..5",
        "language:Verilog stars:1..2",
        # Language: SystemVerilog — split by star range
        "language:SystemVerilog stars:>500",
        "language:SystemVerilog stars:100..500",
        "language:SystemVerilog stars:50..100",
        "language:SystemVerilog stars:20..50",
        "language:SystemVerilog stars:10..20",
        "language:SystemVerilog stars:5..10",
        "language:SystemVerilog stars:2..5",
        "language:SystemVerilog stars:1..2",
        # Topic-based searches
        "topic:verilog stars:>1",
        "topic:systemverilog stars:>1",
        "topic:fpga language:Verilog stars:>1",
        "topic:risc-v language:Verilog stars:>1",
        "topic:riscv language:Verilog stars:>1",
        "topic:rtl language:Verilog stars:>1",
        "topic:hardware language:Verilog stars:>1",
        "topic:asic language:Verilog stars:>1",
        "topic:hdl language:Verilog stars:>1",
        "topic:fpga language:SystemVerilog stars:>1",
        "topic:risc-v language:SystemVerilog stars:>1",
        "topic:asic language:SystemVerilog stars:>1",
        "topic:rtl language:SystemVerilog stars:>1",
        "topic:hdl language:SystemVerilog stars:>1",
        # Keyword-based searches for less tagged repos
        "verilog cpu stars:>5 language:Verilog",
        "verilog uart stars:>5 language:Verilog",
        "verilog spi stars:>5 language:Verilog",
        "verilog i2c stars:>5 language:Verilog",
        "verilog axi stars:>5 language:Verilog",
        "verilog ddr stars:>5 language:Verilog",
        "verilog ethernet stars:>5 language:Verilog",
        "verilog dma stars:>5 language:Verilog",
        "verilog fifo stars:>5 language:Verilog",
        "verilog pcie stars:>5 language:Verilog",
        "verilog usb stars:>5 language:Verilog",
        "verilog sha stars:>5 language:Verilog",
        "verilog aes stars:>5 language:Verilog",
        "verilog fpu stars:>5 language:Verilog",
        "verilog mips stars:>5 language:Verilog",
        "verilog riscv stars:>5 language:Verilog",
        "verilog hdmi stars:>5 language:Verilog",
        "verilog vga stars:>5 language:Verilog",
        "verilog sdram stars:>5 language:Verilog",
        "verilog pwm stars:>5 language:Verilog",
        "verilog cache stars:>5 language:Verilog",
        "verilog arbiter stars:>5 language:Verilog",
        "verilog pipeline stars:>5 language:Verilog",
        "verilog timer stars:>5 language:Verilog",
        "verilog wishbone stars:>5 language:Verilog",
        "verilog avalon stars:>5 language:Verilog",
        "verilog apb stars:>5 language:Verilog",
        "verilog ahb stars:>5 language:Verilog",
        "verilog fft stars:>5 language:Verilog",
        "verilog cordic stars:>5 language:Verilog",
        "verilog crc stars:>5 language:Verilog",
        "verilog noc stars:>5 language:Verilog",
        "systemverilog cpu stars:>5 language:SystemVerilog",
        "systemverilog uart stars:>5 language:SystemVerilog",
        "systemverilog axi stars:>5 language:SystemVerilog",
        "systemverilog fifo stars:>5 language:SystemVerilog",
        "systemverilog cache stars:>5 language:SystemVerilog",
        "systemverilog dma stars:>5 language:SystemVerilog",
        "systemverilog pipeline stars:>5 language:SystemVerilog",
        "systemverilog pcie stars:>5 language:SystemVerilog",
        "systemverilog usb stars:>5 language:SystemVerilog",
        # Topic-based — specialized
        "topic:opencores language:Verilog",
        "topic:open-source-hardware language:Verilog",
        "topic:tinytapeout language:Verilog",
        "topic:openlane language:Verilog",
        "topic:skywater language:Verilog",
        "topic:caravel language:Verilog",
        "topic:ice40 language:Verilog",
        "topic:ecp5 language:Verilog",
        "topic:xilinx language:Verilog",
        "topic:altera language:Verilog",
        "topic:lattice language:Verilog",
        "topic:gowin language:Verilog",
        "topic:zynq language:Verilog",
        # Date-range buckets — catch repos by creation date
        "language:Verilog created:>2023-01-01 stars:>2",
        "language:Verilog created:2021-01-01..2023-01-01 stars:>2",
        "language:Verilog created:2019-01-01..2021-01-01 stars:>2",
        "language:Verilog created:2017-01-01..2019-01-01 stars:>2",
        "language:Verilog created:2015-01-01..2017-01-01 stars:>2",
        "language:Verilog created:<2015-01-01 stars:>2",
        "language:SystemVerilog created:>2023-01-01 stars:>2",
        "language:SystemVerilog created:2021-01-01..2023-01-01 stars:>2",
        "language:SystemVerilog created:2019-01-01..2021-01-01 stars:>2",
        "language:SystemVerilog created:<2019-01-01 stars:>2",
        # Push-date buckets — recently active repos
        "language:Verilog pushed:>2024-06-01 stars:>1",
        "language:Verilog pushed:2023-01-01..2024-06-01 stars:>1",
        "language:Verilog pushed:2021-01-01..2023-01-01 stars:>1",
        "language:SystemVerilog pushed:>2024-06-01 stars:>1",
        "language:SystemVerilog pushed:2023-01-01..2024-06-01 stars:>1",
        # Size-based buckets — large repos likely have more PRs
        "language:Verilog size:>50000 stars:>1",
        "language:Verilog size:10000..50000 stars:>1",
        "language:Verilog size:5000..10000 stars:>1",
        "language:Verilog size:1000..5000 stars:>1",
        "language:SystemVerilog size:>50000 stars:>1",
        "language:SystemVerilog size:10000..50000 stars:>1",
        "language:SystemVerilog size:1000..10000 stars:>1",
        # Fork-based — popular forked repos
        "language:Verilog forks:>50 stars:>5",
        "language:Verilog forks:10..50 stars:>5",
        "language:SystemVerilog forks:>50 stars:>5",
        "language:SystemVerilog forks:10..50 stars:>5",
    ]

    seen: Set[str] = set()
    out: List[str] = []

    for q in queries:
        print(f"[search] query: {q}", file=sys.stderr)
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
                        out.append(full_name)
                        log(f"[repos] + {full_name} ({spdx})", debug)
                        if len(out) >= max_repos:
                            return out
                time.sleep(0.2)
        except Exception as e:
            print(f"[warn] query '{q}' hit limit or error, moving to next: {e}", file=sys.stderr)
            continue

    return out

def get_repo_license(api: GhApi, owner: str, repo: str, debug: bool) -> Optional[Dict]:
    try:
        lic = api.repos.get_license(owner, repo)
        spdx = (getattr(getattr(lic, "license", None), "spdx_id", "") or "").lower()
        if spdx not in PERMISSIVE:
            log(f"[license] {owner}/{repo} excluded by SPDX={spdx}", debug)
            return None
        content = getattr(lic, "content", None)
        text = ""
        if content:
            try:
                text = base64.b64decode(content).decode("utf-8", errors="ignore")
            except Exception:
                pass
        return {"spdx_id": getattr(lic.license, "spdx_id", ""), "text": text}
    except Exception:
        try:
            r = api.repos.get(owner, repo)
            spdx = (getattr(getattr(r, "license", None), "spdx_id", "") or "").lower()
            if spdx in PERMISSIVE:
                log(f"[license:fallback] {owner}/{repo} SPDX={spdx} (no text)", debug)
                return {"spdx_id": getattr(getattr(r, "license", None), "spdx_id", ""), "text": ""}
        except Exception:
            pass
    log(f"[license] {owner}/{repo} has no permissive license", debug)
    return None

def list_closed_issues(api: GhApi, owner: str, repo: str, cap: int, debug: bool):
    out = []
    for page in paged(api.issues.list_for_repo, owner, repo, state="closed",
                      sort="updated", direction="desc", per_page=100):
        for issue in page or []:
            if len(out) >= cap:
                return out
            if hasattr(issue, "pull_request") and issue.pull_request is not None:
                continue
            out.append(issue)
        time.sleep(0.2)
    log(f"[issues] {owner}/{repo}: {len(out)} closed issues scanned", debug)
    return out

def find_closing_pr(api: GhApi, owner: str, repo: str, issue, debug: bool):
    try:
        for page in paged(api.issues.list_events_for_timeline, owner, repo, issue.number, per_page=100):
            for ev in page or []:
                if getattr(ev, "event", "") in {"cross-referenced", "referenced"}:
                    src = getattr(ev, "source", None)
                    src_issue = getattr(src, "issue", None) if src else None
                    if src_issue and getattr(src_issue, "pull_request", None):
                        prn = getattr(src_issue, "number", None)
                        if prn:
                            pr = api.pulls.get(owner, repo, prn)
                            if getattr(pr, "merged_at", None):
                                return pr
            time.sleep(0.1)
    except Exception:
        pass
    patt = re.compile(rf"(?:close[sd]?|fixe?[sd]?|resolve[sd]?):?\s*#\s*{issue.number}\b", re.I)
    for page in paged(api.pulls.list, owner, repo, state="closed", sort="updated", direction="desc", per_page=100):
        for pr in page or []:
            if not getattr(pr, "merged_at", None):
                continue
            if patt.search(getattr(pr, "body", "") or ""):
                return pr
        time.sleep(0.1)
    log(f"[link] {owner}/{repo} issue #{issue.number}: no linked merged PR", debug)
    return None

def get_changed_verilog_files_with_stats(api: GhApi, owner: str, repo: str, prn: int) -> Tuple[List[str], List[Dict]]:
    files = []
    file_stats = []
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
            time.sleep(0.05)
    except Exception as e:
        print(f"[warn] {owner}/{repo} PR#{prn}: skipping file list ({e})", file=sys.stderr)
        return [], []
    return files, file_stats

def get_pr_full_stats(api: GhApi, owner: str, repo: str, prn: int) -> Dict:
    stats = {
        "total_additions": 0,
        "total_deletions": 0,
        "changed_files": 0,
        "commits": 0,
        "review_comments": 0,
        "comments": 0,
    }
    try:
        pr_detail = api.pulls.get(owner, repo, prn)
        stats["total_additions"] = getattr(pr_detail, "additions", 0)
        stats["total_deletions"] = getattr(pr_detail, "deletions", 0)
        stats["changed_files"] = getattr(pr_detail, "changed_files", 0)
        stats["commits"] = getattr(pr_detail, "commits", 0)
        stats["review_comments"] = getattr(pr_detail, "review_comments", 0)
        stats["comments"] = getattr(pr_detail, "comments", 0)
    except Exception:
        pass
    return stats

def get_file_at_ref(api: GhApi, owner: str, repo: str, path: str, ref: str) -> Optional[str]:
    try:
        obj = api.repos.get_content(owner, repo, path, ref=ref)
        content = getattr(obj, "content", None)
        if content:
            return base64.b64decode(content).decode("utf-8", errors="ignore")
    except Exception:
        return None
    return None

def get_pr_patch(api: GhApi, owner: str, repo: str, prn: int) -> str:
    try:
        resp = api.client.get(f"/repos/{owner}/{repo}/pulls/{prn}",
                              headers={"Accept": "application/vnd.github.v3.patch"},
                              timeout=30)
        return getattr(resp, "text", "") or ""
    except Exception:
        return ""

def collect_issue_centric(api: GhApi, owner: str, repo: str, max_issues: int, debug: bool):
    tasks = []
    issues = list_closed_issues(api, owner, repo, max_issues, debug)
    for issue in issues:
        labels = [getattr(l, "name", "").lower() for l in getattr(issue, "labels", [])]
        if not any(l in {"bug", "fix", "enhancement", "feature"} for l in labels):
            if not re.search(r"\b(bug|fix|feature|enhance|regress|regression)\b", f"{issue.title}\n{issue.body or ''}", re.I):
                continue

        pr = find_closing_pr(api, owner, repo, issue, debug)
        if not pr or not getattr(pr, "merged_at", None):
            continue

        changed, file_stats = get_changed_verilog_files_with_stats(api, owner, repo, pr.number)
        if not changed:
            continue

        pr_stats = get_pr_full_stats(api, owner, repo, pr.number)

        base_sha = getattr(getattr(pr, "base", None), "sha", None)
        head_sha = getattr(getattr(pr, "head", None), "sha", None)
        if not base_sha or not head_sha:
            continue

        context_files, target_files = {}, {}
        for p in changed:
            pre = get_file_at_ref(api, owner, repo, p, base_sha)
            post = get_file_at_ref(api, owner, repo, p, head_sha)
            if pre is not None:
                context_files[p] = pre
            if post is not None:
                target_files[p] = post
            time.sleep(0.03)
        if not context_files:
            continue

        diff = get_pr_patch(api, owner, repo, pr.number)
        comments = []
        try:
            for page in paged(api.issues.list_comments, owner, repo, pr.number, per_page=100):
                for c in page or []:
                    comments.append({
                        "author": getattr(getattr(c, "user", None), "login", None),
                        "date": getattr(c, "created_at", None),
                        "body": getattr(c, "body", None),
                    })
                time.sleep(0.03)
        except Exception:
            pass
        try:
            for page in paged(api.pulls.list_review_comments, owner, repo, pr.number, per_page=100):
                for c in page or []:
                    comments.append({
                        "author": getattr(getattr(c, "user", None), "login", None),
                        "date": getattr(c, "created_at", None),
                        "body": getattr(c, "body", None),
                    })
                time.sleep(0.03)
        except Exception:
            pass

        body_text = f"{issue.title}\n{issue.body or ''}"
        if "bug" in labels or "fix" in labels or re.search(r"\b(bug|fix|regression)\b", body_text, re.I):
            task_type = "bug-fix"
        elif any(l in labels for l in ("enhancement", "feature")) or re.search(r"\b(feature|enhance|add)\b", body_text, re.I):
            task_type = "feature-removal" if re.search(r"\b(remove|deprecat(e|ion|ed))\b", body_text, re.I) else "feature-addition"
        else:
            task_type = "other"

        tasks.append({
            "repo": f"{owner}/{repo}",
            "license": get_repo_license(api, owner, repo, debug),
            "issue": {
                "number": issue.number,
                "title": issue.title,
                "body": issue.body,
                "labels": labels,
                "created_at": getattr(issue, "created_at", None),
                "closed_at": getattr(issue, "closed_at", None),
                "html_url": getattr(issue, "html_url", None),
            },
            "pull_request": {
                "number": pr.number,
                "title": pr.title,
                "body": pr.body,
                "merged_at": getattr(pr, "merged_at", None),
                "html_url": getattr(pr, "html_url", None),
                "base_sha": base_sha,
                "head_sha": head_sha,
                "stats": pr_stats,
            },
            "file_stats": file_stats,
            "context_files": context_files,
            "target_files": target_files,
            "diff": diff,
            "comments": comments,
            "task_type": task_type,
        })
        time.sleep(0.1)
    return tasks

def issues_closed_by_pr(api: GhApi, owner: str, repo: str, pr) -> List[int]:
    nums: Set[int] = set()
    for m in CLOSE_RE.finditer(getattr(pr, "body", "") or ""):
        try: nums.add(int(m.group(1)))
        except: pass
    try:
        for page in paged(api.issues.list_events_for_timeline, owner, repo, pr.number, per_page=100):
            for ev in page or []:
                if getattr(ev, "event", "") in {"cross-referenced", "referenced"}:
                    src = getattr(ev, "source", None)
                    src_issue = getattr(src, "issue", None) if src else None
                    if src_issue and not getattr(src_issue, "pull_request", None):
                        n = getattr(src_issue, "number", None)
                        if isinstance(n, int): nums.add(n)
            time.sleep(0.05)
    except Exception:
        pass
    return sorted(nums)

def list_recent_merged_prs(api: GhApi, owner: str, repo: str, cap: int, debug: bool):
    out = []
    for page in paged(api.pulls.list, owner, repo, state="closed", sort="updated", direction="desc", per_page=100):
        for pr in page or []:
            if getattr(pr, "merged_at", None):
                out.append(pr)
                if len(out) >= cap:
                    return out
        time.sleep(0.1)
    log(f"[prs] {owner}/{repo}: {len(out)} merged PRs scanned", debug)
    return out

def collect_pr_centric(api: GhApi, owner: str, repo: str, max_prs: int, debug: bool):
    tasks = []
    prs = list_recent_merged_prs(api, owner, repo, max_prs, debug)
    for pr in prs:
        changed, file_stats = get_changed_verilog_files_with_stats(api, owner, repo, pr.number)
        if not changed:
            continue

        pr_stats = get_pr_full_stats(api, owner, repo, pr.number)

        base_sha = getattr(getattr(pr, "base", None), "sha", None)
        head_sha = getattr(getattr(pr, "head", None), "sha", None)
        if not base_sha or not head_sha:
            continue

        issue_nums = issues_closed_by_pr(api, owner, repo, pr)
        issues_meta = []
        for n in issue_nums:
            try:
                issues_meta.append(api.issues.get(owner, repo, n))
            except Exception:
                pass

        context_files, target_files = {}, {}
        for p in changed:
            pre = get_file_at_ref(api, owner, repo, p, base_sha)
            post = get_file_at_ref(api, owner, repo, p, head_sha)
            if pre is not None:
                context_files[p] = pre
            if post is not None:
                target_files[p] = post
            time.sleep(0.03)
        if not context_files:
            continue

        diff = get_pr_patch(api, owner, repo, pr.number)

        comments = []
        try:
            for page in paged(api.issues.list_comments, owner, repo, pr.number, per_page=100):
                for c in page or []:
                    comments.append({
                        "author": getattr(getattr(c, "user", None), "login", None),
                        "date": getattr(c, "created_at", None),
                        "body": getattr(c, "body", None),
                    })
                time.sleep(0.03)
        except Exception:
            pass
        try:
            for page in paged(api.pulls.list_review_comments, owner, repo, pr.number, per_page=100):
                for c in page or []:
                    comments.append({
                        "author": getattr(getattr(c, "user", None), "login", None),
                        "date": getattr(c, "created_at", None),
                        "body": getattr(c, "body", None),
                    })
                time.sleep(0.03)
        except Exception:
            pass

        text = f"{pr.title}\n{pr.body or ''}\n" + "\n".join([(im.title or "") + "\n" + (im.body or "") for im in issues_meta])
        if re.search(r"\b(bug|fix|regression)\b", text, re.I):
            task_type = "bug-fix"
        elif re.search(r"\b(remove|deprecat(e|ion|ed))\b", text, re.I):
            task_type = "feature-removal"
        elif re.search(r"\b(feature|enhance|add|implement)\b", text, re.I):
            task_type = "feature-addition"
        else:
            task_type = "other"

        tasks.append({
            "repo": f"{owner}/{repo}",
            "license": get_repo_license(api, owner, repo, debug),
            "issue": [
                {
                    "number": getattr(im, "number", None),
                    "title": getattr(im, "title", None),
                    "body": getattr(im, "body", None),
                    "labels": [getattr(l, "name", "").lower() for l in getattr(im, "labels", [])],
                    "created_at": getattr(im, "created_at", None),
                    "closed_at": getattr(im, "closed_at", None),
                    "html_url": getattr(im, "html_url", None),
                }
                for im in issues_meta
            ],
            "pull_request": {
                "number": pr.number,
                "title": pr.title,
                "body": pr.body,
                "merged_at": getattr(pr, "merged_at", None),
                "html_url": getattr(pr, "html_url", None),
                "base_sha": base_sha,
                "head_sha": head_sha,
                "stats": pr_stats,
            },
            "file_stats": file_stats,
            "context_files": context_files,
            "target_files": target_files,
            "diff": diff,
            "comments": comments,
            "task_type": task_type,
        })
        time.sleep(0.1)
    return tasks

def main():
    p = argparse.ArgumentParser(description="Build a Verilog SWE-bench-style dataset (ghapi)")
    p.add_argument("--output", required=True, help="Output JSONL file")
    p.add_argument("--max_repos",      type=int, default=25,  help="Max repos to collect (ignored with --unlimited)")
    p.add_argument("--max_issues",     type=int, default=60,  help="Max closed issues per repo (ignored with --unlimited)")
    p.add_argument("--max_prs",        type=int, default=120, help="Max merged PRs per repo (ignored with --unlimited)")
    p.add_argument("--max_tasks_per_repo", type=int, default=50, help="Max tasks per repo (ignored with --unlimited)")
    p.add_argument("--unlimited",      action="store_true",   help="Remove ALL caps — scrape every repo/PR/issue found")
    p.add_argument("--pr_first",       action="store_true",   help="Use PR-centric path only (skip issue-centric)")
    p.add_argument("--debug",          action="store_true",   help="Verbose stderr logging")
    p.add_argument("--dry-run",        action="store_true",   help="Do not write JSONL; just log")
    p.add_argument("--skip",           nargs="*", default=[], help="Additional repos to skip e.g. owner/repo")
    args = p.parse_args()

    # ── Unlimited mode: override every cap ──
    if args.unlimited:
        args.max_repos          = UNLIMITED
        args.max_issues         = UNLIMITED
        args.max_prs            = UNLIMITED
        args.max_tasks_per_repo = UNLIMITED
        print("[unlimited] All caps removed — scraping everything found", file=sys.stderr)

    from dotenv import load_dotenv
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("Please set GITHUB_TOKEN in your environment or .env file")

    api = GhApi(token=token)

    # ── RESUME: load already collected repos ──
    already_collected = load_already_collected(args.output)
    for s in args.skip:
        already_collected.add(s)
    if already_collected:
        print(f"[resume] Skipping {len(already_collected)} already-collected repos", file=sys.stderr)

    repos = search_repositories(api, max_repos=args.max_repos, debug=args.debug)
    if not repos:
        print("No repositories matched search criteria.", file=sys.stderr)
        return

    # Filter out already-collected before reporting
    new_repos = [r for r in repos if r not in already_collected]
    print(f"[search] Found {len(repos)} candidate repos | {len(new_repos)} new (not yet scraped)", file=sys.stderr)

    out_path = Path(args.output)
    fout = None if args.dry_run else out_path.open("a", encoding="utf-8")

    total = 0
    for full_name in repos:

        if full_name in already_collected:
            log(f"[skip] {full_name}: already collected", args.debug)
            print(f"[skip] {full_name}: already collected", file=sys.stderr)
            continue

        owner, repo = split_repo_full_name(full_name)
        lic = get_repo_license(api, owner, repo, args.debug)
        if not lic:
            log(f"[skip] {full_name}: non-permissive or missing license", args.debug)
            continue

        tasks = []
        if not args.pr_first:
            tasks.extend(collect_issue_centric(api, owner, repo, args.max_issues, args.debug))
        tasks.extend(collect_pr_centric(api, owner, repo, args.max_prs, args.debug))

        # Deduplicate by PR number (issue-centric and PR-centric can overlap)
        seen_prs: Set[int] = set()
        deduped = []
        for t in tasks:
            prn = (t.get("pull_request") or {}).get("number")
            if prn and prn in seen_prs:
                continue
            if prn:
                seen_prs.add(prn)
            deduped.append(t)
        tasks = deduped

        tasks = [t for t in tasks if t.get("license")]

        if len(tasks) > args.max_tasks_per_repo:
            print(f"[cap] {full_name}: capping {len(tasks)} → {args.max_tasks_per_repo}", file=sys.stderr)
            tasks = tasks[:args.max_tasks_per_repo]

        print(f"[repo] {full_name}: collected {len(tasks)} tasks  (running total: {total + len(tasks)})", file=sys.stderr)
        for dp in tasks:
            if not args.dry_run:
                fout.write(json.dumps(dp, ensure_ascii=False) + "\n")
                fout.flush()
        total += len(tasks)
        time.sleep(0.5)

    if fout:
        fout.flush()
        fout.close()
    print(f"\nDone. Wrote {total} new datapoints to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()