#!/usr/bin/env bash
# Node 2: Initial Setup & HDFS Initialization
# Author: Balreddy (Storage Lead)

set -e

echo "=== [Node 2] Initializing Hadoop & HDFS Cluster ==="

# 1. Copy cluster configurations
cp ../configs/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
cp ../configs/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
cp ../configs/hadoop-env.sh.template /opt/hadoop/etc/hadoop/hadoop-env.sh
cp ../configs/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# 2. Check and format NameNode if not already formatted
if [ ! -d "/opt/hadoop/hdfs/namenode/current" ]; then
    echo "[*] Formatting HDFS NameNode for the first time..."
    hdfs namenode -format -force -nonInteractive
else
    echo "[!] NameNode already formatted. Skipping format to preserve ClusterID."
fi

# 3. Start HDFS daemons on Node 2
echo "[*] Starting HDFS NameNode..."
hdfs --daemon start namenode

echo "[*] Starting HDFS DataNode (Replica 1)..."
hdfs --daemon start datanode

sleep 3

# 4. Create base HDFS directories
echo "[*] Creating required HDFS paths..."
hdfs dfs -mkdir -p /iceberg
hdfs dfs -mkdir -p /checkpoints
hdfs dfs -mkdir -p /tmp
hdfs dfs -chmod -R 777 /iceberg
hdfs dfs -chmod -R 777 /checkpoints
hdfs dfs -chmod -R 777 /tmp

# 5. Start Spark Worker 1
echo "[*] Starting Spark Worker 1 (Connecting to Spark Master on Node 3)..."
/opt/spark/sbin/start-worker.sh spark://node3:7077

echo "=== [✓] Node 2 is operational! ==="
echo "Check HDFS report: hdfs dfsadmin -report"
