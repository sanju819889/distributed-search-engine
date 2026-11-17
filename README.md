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
                                     │ Exports Parquet (SCP)
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
2. Verify HDFS is healthy:
   ```bash
   hdfs dfsadmin -report
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

---

### 🟢 Sanjeev — Node 1 (Ingestion & Data Quality)
1. Validate dataset before submitting:
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
   python3 ingest.py ../data/changes.jsonl --phase phase1 --master spark://node3:7077
   ```
*(Iceberg's `MERGE INTO` SQL ensures idempotency: running twice does not duplicate records).*

---

### 🟢 Koushik — Node 4 (Search Service & Web UI)
1. Start the search server:
   ```bash
   cd node4_search
   chmod +x *.sh
   ./start_server.sh
   ```
2. Open the Web UI at `http://node4:8000` or `http://localhost:8000`.
3. Verify Phase 0 is loaded and responsive.
4. **Important:** Arrive at evaluation with Phase 0 already running!

---

## 4. Ranking Algorithm Formulas

### BM25 Formulation
$$BM25(D, Q) = \sum_{t \in Q} IDF(t) \times \frac{TF(t, D) \times (k_1 + 1)}{TF(t, D) + k_1 \times \left(1 - b + b \times \frac{len(D)}{avgdl}\right)}$$
* $k_1 = 1.2$
* $b = 0.75$

### Combined Final Ranking Score
$$\text{Final Score} = 0.7 \times \left(\frac{BM25}{\max BM25}\right) + 0.3 \times \left(\frac{PageRank}{\max PageRank}\right)$$

---

## 5. Local Quick Testing (Zero Setup Required)
Test the entire pipeline algorithmically on any machine:
```bash
cd scripts
python3 test_local_pipeline.py
```
