import json, difflib

recs = [json.loads(l) for l in open('verilog_repos.ndjson', encoding='utf-8') if l.strip()]

rec = recs[0]
print("PR:", rec['pull_request']['number'])
print("Title:", rec['pull_request']['title'])
print("Body:", rec['pull_request'].get('body','')[:300])
print()
print("context_files keys:", list(rec['context_files'].keys()))
print("target_files keys:", list(rec['target_files'].keys()))
print()

# Show the actual diff for the first file
for fname in rec['context_files']:
    a = rec['context_files'][fname].splitlines()
    b = rec['target_files'].get(fname, '').splitlines()
    diff = list(difflib.unified_diff(a, b, fromfile='before/'+fname, tofile='after/'+fname, lineterm=''))
    print(f"--- Diff for {fname} ---")
    for line in diff:
        print(line)
