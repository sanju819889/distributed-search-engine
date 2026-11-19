# 🔍 Distributed Search Engine (Mini-Google)
### Big Data 2026 (UE24CS343AB2) — Team Master Documentation

A distributed, end-to-end search engine running across 4 interconnected Ubuntu VMs over a **ZeroTier** private overlay network, featuring **Apache Iceberg**, **HDFS**, **Apache Spark**, and **FastAPI**.

---

## 1. System Architecture & Node Roles

```
                              [ Node 1: Ingestion ]
                                  (Sanjeev)
                                      │
              Uploads JSONL / Submits Spark Ingestion Job
                                      ▼
      ┌──────────────────────────────────────────────────────────────┐
      │                     HDFS & SPARK CLUSTER                     │
      │                                                              │
      │   [ Node 2: Storage Core ]        [ Node 3: Compute Core ]   │
      │        (Balreddy)                       (Abhishek)           │
      │    • HDFS NameNode                  • Spark Master           │
      │    • HDFS DataNode (Replica 1)      • HDFS DataNode (Repl 2) │
      │    • Spark Worker 1                 • Spark Worker 2         │
      │    • Iceberg Tables on HDFS         • Inverted Index Driver  │
      │                                     • PageRank Driver (>=10) │
      └──────────────────────────────┬───────────────────────────────┘
                                     │ Exports Parquet (HDFS / SCP)
                                     ▼
                        [ Node 4: Search Service & UI ]
                                   (Koushik)
                         • FastAPI Web Server (:8000)
                         • In-Memory Parquet Loader
                         • BM25 (k1=1.2, b=0.75) + PageRank Combo
                         • Google-style Web Frontend
```

---

## 2. Network Mapping (`/etc/hosts`)

All nodes connect using ZeroTier. Ensure `/etc/hosts` contains:
```text
10.147.18.222   node1
<Node2_ZT_IP>   node2
<Node3_ZT_IP>   node3
<Node4_ZT_IP>   node4
```

---

## 3. Step-by-Step Execution Guide per Member

### 🟢 Balreddy — Node 2 (Storage Core)
1. Initialize HDFS and start storage daemons:
   ```bash
   cd node2_storage
   chmod +x *.sh
   ./setup_hdfs.sh
   ```
2. Verify HDFS is healthy (2 DataNodes live):
   ```bash
   hdfs dfsadmin -report
   ```
3. Verify `pages.jsonl` is visible in HDFS:
   ```bash
   hdfs dfs -ls -R / | grep -i jsonl
   ```

---

### 🟢 Abhishek — Node 3 (Compute Core)
1. Start Spark Master, Worker, and DataNode:
   ```bash
   cd node3_compute
   chmod +x *.sh
   ./start_compute.sh
   ```
2. Check Spark Master UI in your browser at `http://node3:8080` (verify both workers are alive).
3. Once Sanjeev finishes Ingestion, execute the distributed index and PageRank compute job:
   ```bash
   python3 compute_pipeline.py --version v0 --master spark://node3:7077
   ```
4. Transfer exported Parquet files to Node 4:
   ```bash
   ./transfer_to_node4.sh v0
   ```
5. During Live Evaluation Demo (Phase 1):
   ```bash
   python3 compute_pipeline.py --version v1 --master spark://node3:7077
   ./transfer_to_node4.sh v1
   ```

---

### 🟢 Sanjeev — Node 1 (Ingestion & Data Quality)
1. Validate dataset locally before submitting:
   ```bash
   cd node1_ingestion
   python3 validate_local.py ../data/pages.jsonl
   ```
2. Run Phase 0 Ingestion into Apache Iceberg table:
   ```bash
   python3 ingest.py ../data/pages.jsonl --phase phase0 --master spark://node3:7077
   ```
3. During the Live Evaluation Demo (Phase 1):
   ```bash
   python3 ingest.py ../data/changes.json --phase phase1 --master spark://node3:7077
   ```
*(Iceberg's `MERGE INTO` SQL guarantees idempotency: running multiple times never duplicates records).*

---

### 🟢 Koushik — Node 4 (Search Service & Web UI)
1. Start the search server:
   ```bash
   cd node4_search
   chmod +x *.sh
   ./start_server.sh
   ```
2. Open the Web UI at `http://node4:8000` or `http://localhost:8000`.
3. Verify Phase 0 baseline is serving:
   ```bash
   curl -s "http://localhost:8000/health"
   curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool
   ```
4. **Important:** Arrive at evaluation with Phase 0 already running!

---

## 4. Evaluation Verification Checklist (Matches Notion Runbook)

| Step | Node | Command | Expected Output |
| :--- | :--- | :--- | :--- |
| **HDFS Health** | VM2 | `hdfs dfsadmin -report` | 2 Live DataNodes (`node2`, `node3`) |
| **HDFS File** | VM2 | `hdfs dfs -ls -R / \| grep -i jsonl` | `/data/pages.jsonl` exists |
| **Phase 0 Search** | VM4 | `curl -s "http://localhost:8000/search?q=beard&limit=5"` | Doc 94 title is `"Beard"` |
| **Phase 0 PR** | VM3 | `spark-sql -e "SELECT doc_id, rank FROM iceberg_catalog.lake.pagerank WHERE doc_id = '146';"` | Baseline score for doc 146 |
| **Phase 1 Ingest** | VM1 | `spark-sql -e "SELECT snapshot_id, committed_at FROM iceberg_catalog.lake.pages.history;"` | $\ge 2$ snapshots |
| **Phase 1 Index** | VM3 | `spark-sql -e "SELECT term, doc_id FROM iceberg_catalog.lake.inverted_index WHERE doc_id = 'phase1-new-001' LIMIT 5;"` | Terms for new document |
| **Phase 1 Delete**| VM3 | `spark-sql -e "SELECT COUNT(*) FROM iceberg_catalog.lake.pagerank WHERE doc_id = '1';"` | Expect `0` |
| **Parquet Export** | VM2 | `hdfs dfs -ls /serving/v0/` and `/serving/v1/` | `docs.parquet`, `index.parquet` |
| **Health Ready** | VM4 | `curl -s http://localhost:8000/health` | `{"status":"ok","version":1}` |
| **Updated Title** | VM4 | `curl -s "http://localhost:8000/search?q=beard&limit=5"` | Doc 94 title is `"Beard (Phase 1)"` |
| **PR Gain** | VM4/3 | `curl -s "http://localhost:8000/search?q=cooking&limit=5"` | Doc 146 rank higher than baseline |
| **Multi-word** | VM4 | `curl -s "http://localhost:8000/search?q=plant+growth&limit=5"` | Valid JSON response, no 500 |
| **Idempotency** | VM3 | `spark-sql -e "SELECT COUNT(*) FROM iceberg_catalog.lake.pages;"` | Count identical after re-running |

---

## 5. Ranking Algorithm Formulas

### BM25 Formulation
$$BM25(D, Q) = \sum_{t \in Q} IDF(t) \times \frac{TF(t, D) \times (k_1 + 1)}{TF(t, D) + k_1 \times \left(1 - b + b \times \frac{len(D)}{avgdl}\right)}$$
* $k_1 = 1.2$
* $b = 0.75$

### Combined Final Ranking Score
$$\text{Final Score} = 0.7 \times \left(\frac{BM25}{\max BM25}\right) + 0.3 \times \left(\frac{PageRank}{\max PageRank}\right)$$

---

## 6. Local Quick Simulation
```bash
cd scripts
python3 test_local_pipeline.py
```
