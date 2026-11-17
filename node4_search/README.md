# Node 4: Search Service & Web UI Guide (Koushik)

Node 4 hosts the FastAPI search service and Google-like Web UI. It loads exported plain Parquet files into memory, executes BM25 ranking ($k_1=1.2, b=0.75$), and combines it with PageRank scores.

> [!IMPORTANT]
> **Evaluation Rubric Requirement:** Teams must arrive with Phase 0 already executed and serving on VM4. The search service on VM4 must already be running, hosting the Phase 0 Parquet export in memory.

## 1. Quick Start
```bash
chmod +x start_server.sh
./start_server.sh
```
Open your browser at `http://node4:8000` or `http://localhost:8000`.

## 2. Dynamic Version Reload (Phase 1 Demo)
When Abhishek transfers `/serving/v1/` to this node during the evaluation demo, switch versions without downtime:
```bash
curl -X POST "http://localhost:8000/api/reload?version=v1"
```
The web UI and API will immediately start serving Phase 1 results!
