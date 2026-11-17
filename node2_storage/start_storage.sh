#!/usr/bin/env bash
# Node 2: Start daemons
echo "[*] Starting HDFS NameNode & DataNode on Node 2..."
hdfs --daemon start namenode
hdfs --daemon start datanode

echo "[*] Starting Spark Worker 1..."
/opt/spark/sbin/start-worker.sh spark://node3:7077

echo "[✓] Node 2 services started."
hdfs dfsadmin -report | grep -E "Configured Capacity|Live datanodes"
