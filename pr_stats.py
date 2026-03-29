import json, os, difflib

def classify_file(fname):
    fname = fname.lower()
    if fname.endswith('.v') or fname.endswith('.sv'):
        return 'design'
    if 'tb' in fname or 'test' in fname:
        return 'testbench'
    if fname in ('makefile',) or fname.endswith('.mk') or fname.endswith('.tcl'):
        return 'build'
    if fname.endswith('.md') or fname.endswith('.rst') or fname.endswith('.txt'):
        return 'doc'
    if fname.endswith('.png') or fname.endswith('.jpg') or fname.endswith('.svg'):
        return 'image'
    return 'other'

recs = [json.loads(l) for l in open('verilog_repos.ndjson', encoding='utf-8') if l.strip()]

for rec in recs[:5]:
    repo = rec.get('repo', '?')
    pr = rec.get('pull_request', {})
    pr_num = pr.get('number', '?')
    context = rec.get('context_files', {})
    target = rec.get('target_files', {})

    all_files = set(list(context.keys()) + list(target.keys()))
    total_added = 0
    total_deleted = 0
    counts = {'design':0,'testbench':0,'build':0,'doc':0,'image':0,'other':0}

    for fname in all_files:
        a = context.get(fname, '').splitlines()
        b = target.get(fname, '').splitlines()
        added = sum(1 for line in difflib.ndiff(a, b) if line.startswith('+ '))
        deleted = sum(1 for line in difflib.ndiff(a, b) if line.startswith('- '))
        total_added += added
        total_deleted += deleted
        counts[classify_file(os.path.basename(fname))] += 1

    print(f"Table: PR #{pr_num} Statistics ({repo})")
    print(f"{'Metric':<30} {'Value'}")
    print('-'*40)
    print(f"{'Total files changed':<30} {len(all_files)}")
    print(f"{'Lines added':<30} {total_added}")
    print(f"{'Lines deleted':<30} {total_deleted}")
    print(f"{'Design files (.v)':<30} {counts['design']}")
    print(f"{'Testbench files':<30} {counts['testbench']}")
    print(f"{'Build system files':<30} {counts['build']}")
    print(f"{'Documentation files':<30} {counts['doc']}")
    print(f"{'Image files':<30} {counts['image']}")
    print(f"{'Other auxiliary':<30} {counts['other']}")
    print()
