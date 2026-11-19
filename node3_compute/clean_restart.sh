#!/usr/bin/env bash
# Node 3: 100% Fresh Start & Clean Slate
# Author: Abhishek (Compute Lead)

echo "=== [Node 3] Stopping all running daemons & cleaning stale state ==="

# 1. Stop all existing daemons
/opt/spark/sbin/stop-worker.sh || true
/opt/spark/sbin/stop-master.sh || true
hdfs --daemon stop datanode || true
pkill -9 -f "org.apache.hadoop" || true
pkill -9 -f "org.apache.spark" || true

sleep 2

# 2. Wipe stale DataNode directory to match Node 2's fresh ClusterID
echo "[*] Wiping old DataNode blocks..."
rm -rf /opt/hadoop/hdfs/datanode
rm -rf /tmp/hadoop-*

# 3. Re-start services
echo "[*] Starting fresh Spark Master, Worker & DataNode 2..."
./start_compute.sh
