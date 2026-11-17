# Node 2: Storage & Lakehouse Core Guide (Balreddy)

Node 2 is responsible for the distributed storage foundation (HDFS NameNode + DataNode 1) and storing the Apache Iceberg tables.

## 1. Prerequisites
Ensure Java 11, Hadoop 3.3.6, Spark 3.5.1, and the Iceberg runtime JAR are in `/opt/spark/jars/`.

## 2. First-time Setup
Run this once to configure and initialize HDFS:
```bash
chmod +x setup_hdfs.sh start_storage.sh stop_storage.sh
./setup_hdfs.sh
```

## 3. Daily Usage
- **Start Storage:** `./start_storage.sh`
- **Stop Storage:** `./stop_storage.sh`

## 4. Verification
- Verify HDFS status:
  ```bash
  hdfs dfsadmin -report
  ```
  *(Should report 2 Live DataNodes once Abhishek on Node 3 starts his DataNode).*
- Inspect Iceberg directory on HDFS:
  ```bash
  hdfs dfs -ls -R /iceberg
  ```
