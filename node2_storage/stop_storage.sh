#!/usr/bin/env bash
# Node 2: Stop daemons
echo "[*] Stopping Spark Worker 1..."
/opt/spark/sbin/stop-worker.sh || true

echo "[*] Stopping HDFS DataNode & NameNode..."
hdfs --daemon stop datanode || true
hdfs --daemon stop namenode || true

echo "[✓] Node 2 services stopped."
