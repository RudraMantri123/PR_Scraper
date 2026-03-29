import json

recs = [json.loads(l) for l in open('verilog_repos.ndjson', encoding='utf-8') if l.strip()]

rec = recs[0]
print("Top-level keys:", list(rec.keys()))
print()
print("diff value (first 200 chars):", repr(rec.get('diff',''))[:200])
print()
print("issue keys:", list(rec['issue'][0].keys()) if rec.get('issue') else "no issue")
print()
# Print full first record to see structure
for k, v in rec.items():
    if isinstance(v, str):
        print(f"  {k}: {repr(v[:300])}")
    elif isinstance(v, list):
        print(f"  {k}: list of {len(v)} items")
        if v:
            print(f"    first item keys: {list(v[0].keys()) if isinstance(v[0], dict) else repr(v[0])[:100]}")
    else:
        print(f"  {k}: {repr(v)[:100]}")
