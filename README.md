# Verilog Dataset Builder & High-Speed Scraper

A high-performance toolset to build SWE-bench-style datasets from Verilog/SystemVerilog repositories on GitHub. Now featuring a multi-threaded, distributed architecture for massive scale (20,000+ repositories).

## 🚀 New: High-Speed Tools
We have added specialized scripts for ultra-fast, distributed data collection:
- `fast_dataset_builder.py`: Multi-threaded PR & source code scraper with 10x throughput.
- `fast_issue_extractor.py`: Dedicated scraper for extracting all historical issues (bug reports).
- `TokenManager`: Native support for rotating multiple GitHub tokens to bypass rate limits.

## Setup

### 1. Environment Setup
```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. GitHub Token Configuration
Create a `.env` file in the project root. To maximize speed, you can provide multiple tokens:
```bash
# .env
GITHUB_TOKENS=token1,token2,token3,token4
```

## 🛠️ Performance Usage (Recommended)

### Distributed PR Scraping (Split Across Multiple PCs)
To scrape 20,000+ repos, you can run the script on multiple systems simultaneously. The script will automatically partition the workload using math so no repo is repeated.

**On System 1:**
```bash
python fast_dataset_builder.py --output data_pc1.ndjson --workers 16 --total-nodes 3 --node-index 1
```
**On System 2:**
```bash
python fast_dataset_builder.py --output data_pc2.ndjson --workers 16 --total-nodes 3 --node-index 2
```
**On System 3:**
```bash
python fast_dataset_builder.py --output data_pc3.ndjson --workers 16 --total-nodes 3 --node-index 3
```

### All-Issues Extraction
To extract every single historical issue from a list of repositories:
```bash
python fast_issue_extractor.py --input repos_list.ndjson --output issues_data.ndjson
```

## 📊 Analytics
Once collected, use the statistics tool to analyze your dataset:
```bash
python pr_stats_all.py --input your_data.ndjson --output stats.csv --json stats.json
```

---

## Technical Details: Bypassing Rate Limits
1. **Search API Rotation**: The new search engine rotates tokens during the discovery phase to bypass the GitHub 30-search/min limit.
2. **Socket Timeouts**: All connections have a hard 20s timeout to prevent worker threads from freezing on dropped packets.
3. **Diversification Cap**: We limit PR extraction to 50 per repo to ensure a diverse dataset across thousands of projects.
