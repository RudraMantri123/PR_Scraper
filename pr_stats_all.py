"""
pr_stats_all.py  —  Enhanced PR statistics + file classification
Improvements over original:
  - Content-based testbench detection (`timescale, $finish, $dumpfile, initial begin …)
  - Path-pattern testbench detection (tb/, sim/, verif/, uvm/ …)
  - Gitignore-aware sim-dir detection via GitHub API (cached per repo)
  - Accurate line diff (unified diff parser instead of set subtraction)
  - More file categories: constraint, memory, script, config
  - Per-task-type summary table
  - Richer CSV columns + optional JSON sidecar
  - Progress bar without external deps
  - Graceful handling of missing/empty files
"""

from tempfile import tempdir
import json, os, csv, sys, re, base64, time, argparse
from collections import defaultdict
from pathlib import Path

sys.setrecursionlimit(100_000)

# ── Optional GitHub API (for .gitignore fetching) ──────────────────────────
try:
    from ghapi.all import GhApi
    from dotenv import load_dotenv
    load_dotenv()
    _token = os.getenv("GITHUB_TOKEN")
    _api   = GhApi(token=_token) if _token else None
except ImportError:
    _api = None

# ── Regex patterns ──────────────────────────────────────────────────────────
TB_PATH_RE = re.compile(
    r'(^|[/\\])('
    r'tb_|_tb[._/]|_tb$|testbench[_/]|_testbench|'
    r'test_|_test[._/]|_test$|tests[/\\]|'
    r'sim[/\\]|_sim[._/]|sim_|'
    r'verif[/\\]|verification[/\\]|'
    r'uvm[/\\]|svunit[/\\]|'
    r'spec[/\\]|bfm[/\\]'
    r')', re.I
)

TB_CONTENT_RE = re.compile(
    r'(`timescale\b'
    r'|\$dumpfile\b|\$dumpvars\b|\$dumpon\b|\$dumpoff\b'
    r'|\$finish\b|\$stop\b'
    r'|\$monitor\b|\$strobe\b'
    r'|\$random\b|\$urandom\b'
    r'|\binitial\s+begin\b'
    r'|\bforever\s+begin\b'
    r'|#\s*[0-9]+\s*;'          # delay statements
    r'|\bwait\s*\(.*\)\s*;'
    r')',
    re.I
)

# ── File extension → category map ───────────────────────────────────────────
EXT_MAP = {
    # HDL
    '.v':    'design',  '.sv':   'design',
    '.vh':   'design',  '.svh':  'design',
    # Testbench hint (extension alone — content check confirms)
    # Build / flow
    '.tcl':  'script',  '.xdc':  'constraint',
    '.sdc':  'constraint', '.upf': 'constraint',
    '.qsf':  'script',  '.qpf':  'script',
    '.ys':   'script',  '.f':    'script',
    '.mk':   'build',   '.cmake':'build',
    # Memory / init
    '.mem':  'memory',  '.hex':  'memory',
    '.mif':  'memory',  '.coe':  'memory',
    # Config / project
    '.json': 'config',  '.yaml': 'config',
    '.yml':  'config',  '.toml': 'config',
    '.cfg':  'config',  '.ini':  'config',
    # Docs
    '.md':   'doc',     '.rst':  'doc',
    '.txt':  'doc',     '.pdf':  'doc',
    # Images
    '.png':  'image',   '.jpg':  'image',
    '.svg':  'image',   '.gif':  'image',
    # Python / shell scripts
    '.py':   'script',  '.sh':   'script',
    '.bash': 'script',  '.zsh':  'script',
}

NAME_MAP = {
    'makefile': 'build',
    'gnumakefile': 'build',
    '.gitignore': 'config',
    '.gitattributes': 'config',
}

ALL_CATEGORIES = [
    'design', 'testbench', 'script', 'constraint',
    'memory', 'build', 'config', 'doc', 'image', 'other'
]

# ── Gitignore cache ──────────────────────────────────────────────────────────
_gitignore_cache: dict = {}   # repo → set of sim-dir prefixes

def fetch_gitignore_sim_dirs(repo: str) -> set:
    """Return set of path prefixes flagged as sim-related in .gitignore."""
    if repo in _gitignore_cache:
        return _gitignore_cache[repo]
    sim_dirs = set()
    if _api:
        owner, reponame = repo.split('/', 1)
        try:
            obj     = _api.repos.get_content(owner, reponame, '.gitignore')
            content = getattr(obj, 'content', None)
            if content:
                text = base64.b64decode(content).decode('utf-8', errors='ignore')
                for line in text.splitlines():
                    line = line.strip().rstrip('/')
                    if not line or line.startswith('#'):
                        continue
                    if any(k in line.lower() for k in
                           ['sim', 'tb', 'test', 'wave', 'vcd', 'dump', 'build', 'verif']):
                        sim_dirs.add(line.lstrip('/'))
            time.sleep(0.05)
        except Exception:
            pass
    _gitignore_cache[repo] = sim_dirs
    return sim_dirs

# ── File classifier ──────────────────────────────────────────────────────────
def classify_file(fpath: str, content: str, sim_dirs: set) -> str:
    """Return one of ALL_CATEGORIES for the given file."""
    base  = os.path.basename(fpath).lower()
    ext   = os.path.splitext(base)[1]
    fpath_lower = fpath.lower().replace('\\', '/')

    # Name-exact matches
    if base in NAME_MAP:
        return NAME_MAP[base]

    # Gitignore sim-dir match
    for d in sim_dirs:
        dl = d.lower()
        if fpath_lower.startswith(dl + '/') or ('/' + dl + '/') in fpath_lower:
            # Still confirm it's an HDL file before calling it testbench
            if ext in ('.v', '.sv', '.vh', '.svh'):
                return 'testbench'

    # Path-pattern testbench detection
    if ext in ('.v', '.sv', '.vh', '.svh') and TB_PATH_RE.search(fpath):
        return 'testbench'

    # Extension map
    cat = EXT_MAP.get(ext)

    # Content-based testbench detection (HDL files only)
    if cat == 'design' and content and TB_CONTENT_RE.search(content):
        return 'testbench'

    return cat if cat else 'other'


# ── Accurate line diff counter ───────────────────────────────────────────────
def count_line_changes(before: str, after: str):
    """
    Count added/deleted lines using the unified diff of the two texts.
    Falls back to set-difference if diff module unavailable.
    """
    import difflib
    before_lines = before.splitlines(keepends=True) if before else []
    after_lines  = after.splitlines(keepends=True)  if after  else []
    added = deleted = 0
    for line in difflib.unified_diff(before_lines, after_lines, lineterm=''):
        if line.startswith('+') and not line.startswith('+++'):
            added += 1
        elif line.startswith('-') and not line.startswith('---'):
            deleted += 1
    return added, deleted


# ── Progress bar (no external deps) ─────────────────────────────────────────
def progress(current, total, width=40):
    pct  = current / total if total else 0
    done = int(width * pct)
    bar  = '█' * done + '░' * (width - done)
    print(f'\r[{bar}] {current}/{total} ({pct:.0%})', end='', flush=True)


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Enhanced PR stats extractor")
    parser.add_argument('--input',   default='verilog_repos.ndjson', help='Input NDJSON file')
    parser.add_argument('--output',  default='pr_stats_all.csv',     help='Output CSV file')
    parser.add_argument('--json',    default='pr_stats_all.json',    help='Output JSON sidecar (summary)')
    parser.add_argument('--no-api',  action='store_true',            help='Skip GitHub API .gitignore fetch')
    args = parser.parse_args()

    if args.no_api:
        global _api
        _api = None

    # ── Load records ──
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: {args.input} not found", file=sys.stderr)
        sys.exit(1)

    recs = [json.loads(l) for l in input_path.read_text(encoding='utf-8').splitlines() if l.strip()]
    print(f"Loaded {len(recs)} records from {args.input}")

    rows = []
    task_type_summary = defaultdict(lambda: defaultdict(int))

    for i, rec in enumerate(recs):
        progress(i + 1, len(recs))

        repo      = rec.get('repo', '?')
        pr        = rec.get('pull_request', {}) or {}
        pr_num    = pr.get('number', '?')
        pr_title  = (pr.get('title') or '').replace('\n', ' ')
        task_type = rec.get('task_type', 'other')
        pr_stats  = pr.get('stats', {}) or {}

        context   = rec.get('context_files', {}) or {}
        target    = rec.get('target_files',  {}) or {}
        all_files = sorted(set(list(context.keys()) + list(target.keys())))

        # Fetch .gitignore sim dirs for this repo
        sim_dirs  = fetch_gitignore_sim_dirs(repo) if not args.no_api else set()

        total_added = total_deleted = 0
        counts = {c: 0 for c in ALL_CATEGORIES}
        tb_files  = []
        rtl_files = []
        file_detail = []

        for fpath in all_files:
            before  = context.get(fpath, '') or ''
            after   = target.get(fpath,  '') or ''
            added, deleted = count_line_changes(before, after)
            total_added   += added
            total_deleted += deleted
            cat = classify_file(fpath, before or after, sim_dirs)
            counts[cat] += 1
            if cat == 'testbench': tb_files.append(fpath)
            elif cat == 'design':  rtl_files.append(fpath)
            file_detail.append({'file': fpath, 'category': cat,
                                 'lines_added': added, 'lines_deleted': deleted})

        row = {
            'repo':              repo,
            'pr_number':         pr_num,
            'pr_title':          pr_title,
            'task_type':         task_type,
            'total_files':       len(all_files),
            'lines_added':       total_added,
            'lines_deleted':     total_deleted,
            # PR-level stats from GitHub API
            'pr_additions':      pr_stats.get('total_additions', ''),
            'pr_deletions':      pr_stats.get('total_deletions', ''),
            'pr_commits':        pr_stats.get('commits', ''),
            'pr_review_comments':pr_stats.get('review_comments', ''),
            'pr_comments':       pr_stats.get('comments', ''),
            # File category counts
            **{f'{c}_files': counts[c] for c in ALL_CATEGORIES},
            # Convenience flags
            'has_testbench':     int(len(tb_files) > 0),
            'tb_file_list':      '|'.join(tb_files),
            'rtl_file_list':     '|'.join(rtl_files),
        }
        rows.append(row)

        # Accumulate per-task-type summary
        tt = task_type_summary[task_type]
        tt['count'] += 1
        tt['lines_added']    += total_added
        tt['lines_deleted']  += total_deleted
        tt['has_testbench']  += int(len(tb_files) > 0)
        for c in ALL_CATEGORIES:
            tt[f'{c}_files'] += counts[c]

    print()  # newline after progress bar

    # ── Write CSV ──
    if rows:
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"Saved {len(rows)} rows → {args.output}")

    # ── Write JSON summary sidecar ──
    summary = {
        'total_records':    len(rows),
        'unique_repos':     len(set(r['repo'] for r in rows)),
        'total_files':      sum(r['total_files'] for r in rows),
        'total_lines_added':    sum(r['lines_added'] for r in rows),
        'total_lines_deleted':  sum(r['lines_deleted'] for r in rows),
        'prs_with_testbench':   sum(r['has_testbench'] for r in rows),
        'prs_rtl_only':         sum(1 for r in rows if not r['has_testbench']),
        'category_totals': {
            c: sum(r[f'{c}_files'] for r in rows) for c in ALL_CATEGORIES
        },
        'by_task_type': {k: dict(v) for k, v in task_type_summary.items()}
    }
    with open(args.json, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    print(f"Saved summary   → {args.json}")

    # ── Print summary table ──
    print("\n── Summary ─────────────────────────────────────")
    print(f"  Total PRs:            {summary['total_records']}")
    print(f"  Unique repos:         {summary['unique_repos']}")
    print(f"  PRs with testbench:   {summary['prs_with_testbench']}")
    print(f"  PRs RTL only:         {summary['prs_rtl_only']}")
    print(f"\n── File categories ─────────────────────────────")
    for c, n in summary['category_totals'].items():
        print(f"  {c:<12}: {n}")
    print(f"\n── By task type ────────────────────────────────")
    for tt, stats in summary['by_task_type'].items():
        print(f"  {tt} ({stats['count']} PRs) — "
              f"+{stats['lines_added']} / -{stats['lines_deleted']} lines, "
              f"{stats['has_testbench']} with TB")

if __name__ == '__main__':
    main()
