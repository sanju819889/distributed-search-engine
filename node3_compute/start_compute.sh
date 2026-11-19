#!/usr/bin/env bash
# Node 3: Start daemons
# Author: Abhishek (Compute Lead)

echo "[*] Copying configurations on Node 3..."
cp ../configs/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
cp ../configs/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
cp ../configs/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Setup spark-config for evaluators
mkdir -p ~/spark-config
cp ../configs/iceberg.conf ~/spark-config/iceberg.conf

echo "[*] Starting HDFS DataNode (Replica 2 on Node 3)..."
hdfs --daemon start datanode

echo "[*] Starting Spark Master (Port 7077, Web UI on 8080)..."
/opt/spark/sbin/start-master.sh

echo "[*] Starting Spark Worker 2..."
/opt/spark/sbin/start-worker.sh spark://node3:7077

echo "=== [✓] Node 3 Compute daemons running! ==="
echo "Spark Master: spark://node3:7077"
echo "Spark Web UI: http://node3:8080"
echo "Evaluator config ready at ~/spark-config/iceberg.conf"
