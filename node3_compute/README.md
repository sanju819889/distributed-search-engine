# Node 3: Compute & Ranking Lead Guide (Abhishek)

Node 3 manages the Spark Master, executes distributed Inverted Indexing, runs distributed PageRank ($\ge$ 10 iterations with checkpointing), and exports plain Parquet files to Node 4.

## 1. Start Services
```bash
chmod +x start_compute.sh stop_compute.sh transfer_to_node4.sh
./start_compute.sh
```
Check Spark UI at `http://node3:8080` (should list 2 registered workers: Node 2 and Node 3).

## 2. Execute Phase 0 Compute Job
Once Sanjeev finishes ingestion of Phase 0, run:
```bash
python3 compute_pipeline.py --version v0 --master spark://node3:7077
```

## 3. Transfer Exported Parquet to Node 4
```bash
./transfer_to_node4.sh v0
```

## 4. Live Demo (Phase 1)
When evaluators trigger Phase 1 (`changes.jsonl`):
```bash
# 1. Run compute for v1
python3 compute_pipeline.py --version v1 --master spark://node3:7077

# 2. Transfer to Node 4
./transfer_to_node4.sh v1
```
