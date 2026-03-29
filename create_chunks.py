import os, sys, urllib.request, urllib.parse, json, time, threading
from pathlib import Path

VERILOG_EXT = (".v", ".sv", ".vh", ".svh")
PERMISSIVE = {"mit", "apache-2.0", "bsd-2-clause", "bsd-3-clause", "isc", "unlicense", "zlib"}

from dotenv import load_dotenv
load_dotenv()
tokens_str = os.getenv("GITHUB_TOKENS") or os.getenv("GITHUB_TOKEN")
token_list = [t.strip() for t in tokens_str.split(",") if t.strip()]

lock = threading.Lock()
token_idx = 0

def get_token():
    global token_idx
    with lock:
        t = token_list[token_idx % len(token_list)]
        token_idx += 1
        return t

queries = [
    'language:Verilog stars:>500', 'language:Verilog stars:200..500', 'language:Verilog stars:100..199',
    'language:Verilog stars:50..99', 'language:Verilog stars:30..49', 'language:Verilog stars:20..29',
    'language:Verilog stars:10..19', 'language:Verilog stars:5..9', 'language:Verilog stars:2..4',
    'language:Verilog stars:1..1', 'language:SystemVerilog stars:>200', 'language:SystemVerilog stars:100..200',
    'language:SystemVerilog stars:50..99', 'language:SystemVerilog stars:20..49', 'language:SystemVerilog stars:10..19',
    'language:SystemVerilog stars:5..9', 'language:SystemVerilog stars:2..4', 'language:SystemVerilog stars:1..1',
]

seen = set()
repos_metadata = []

print("Running 4-Token high velocity search to get ALL repositories...", file=sys.stderr)
try:
    for q in queries:
        for page in range(1, 11):
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
                        if spdx in PERMISSIVE:
                            seen.add(full_name)
                            repos_metadata.append(full_name)
            except Exception:
                time.sleep(2)
                continue
except Exception as e:
    pass

print(f"Found {len(repos_metadata)} unique permissively licensed repos.")

import math
chunk_size = math.ceil(len(repos_metadata) / 3)

for i in range(3):
    chunk = repos_metadata[i*chunk_size : (i+1)*chunk_size]
    with open(f"chunk_{i+1}.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(chunk) + "\n")
    print(f"chunk_{i+1}.txt written with {len(chunk)} repos.")
