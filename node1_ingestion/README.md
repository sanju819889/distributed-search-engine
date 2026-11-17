# Node 1: Ingestion & Data Quality Guide (Sanjeev)

Node 1 is the gateway client. It parses, validates, deduplicates, and merges documents into the Apache Iceberg lakehouse table on HDFS.

## 1. Quick Data Validation (Local)
Test your input file before submitting to the cluster:
```bash
python3 validate_local.py ../data/pages.jsonl
```

## 2. Ingestion Submission to Spark Master (Node 3)
Run Phase 0:
```bash
python3 ingest.py ../data/pages.jsonl --phase phase0 --master spark://node3:7077 --warehouse hdfs://node2:9000/iceberg
```

Run Phase 1 (Live Demo):
```bash
python3 ingest.py ../data/changes.jsonl --phase phase1 --master spark://node3:7077 --warehouse hdfs://node2:9000/iceberg
```

### Idempotency Guarantee
The script uses Iceberg's `MERGE INTO` SQL syntax. Ingesting Phase 0 twice will not produce duplicate records or inflate document counts.
