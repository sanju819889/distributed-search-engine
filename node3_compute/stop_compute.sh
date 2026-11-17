#!/usr/bin/env bash
# Node 3: Stop daemons
echo "[*] Stopping Spark Master & Worker..."
/opt/spark/sbin/stop-worker.sh || true
/opt/spark/sbin/stop-master.sh || true

echo "[*] Stopping HDFS DataNode on Node 3..."
hdfs --daemon stop datanode || true

echo "[✓] Node 3 compute services stopped."
