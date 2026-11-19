#!/usr/bin/env bash
# Node 2: 100% Fresh Start & Clean Slate
# Author: Balreddy (Storage Lead)

echo "=== [Node 2] Stopping all running daemons & cleaning stale state ==="

# 1. Stop all existing daemons
/opt/spark/sbin/stop-worker.sh || true
hdfs --daemon stop datanode || true
hdfs --daemon stop namenode || true
pkill -9 -f "org.apache.hadoop" || true
pkill -9 -f "org.apache.spark" || true

sleep 2

# 2. Wipe stale HDFS data directories to avoid ClusterID mismatch
echo "[*] Wiping old NameNode & DataNode blocks..."
rm -rf /opt/hadoop/hdfs/namenode
rm -rf /opt/hadoop/hdfs/datanode
rm -rf /tmp/hadoop-*

# 3. Re-run setup
echo "[*] Running fresh HDFS initialization..."
./setup_hdfs.sh
