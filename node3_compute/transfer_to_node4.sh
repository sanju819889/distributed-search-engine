#!/usr/bin/env bash
# Node 3: Transfer exported Parquet files to Node 4 (Serving Node)
# Usage: ./transfer_to_node4.sh [v0|v1]

VERSION=${1:-v0}
LOCAL_DIR="/serving/${VERSION}"

echo "=== Syncing ${VERSION} Parquet Export to Node 4 ==="

# 1. If local folder doesn't exist, fetch from HDFS
if [ ! -d "$LOCAL_DIR" ]; then
    echo "[*] Fetching from HDFS (/serving/${VERSION})..."
    mkdir -p /serving
    hdfs dfs -get /serving/${VERSION} /serving/
fi

# 2. Transfer to Node 4 over SSH
echo "[*] Copying to Node 4: /serving/${VERSION}..."
ssh -o StrictHostKeyChecking=no node4 "sudo mkdir -p /serving/${VERSION} && sudo chmod -R 777 /serving"
scp -r /serving/${VERSION}/* node4:/serving/${VERSION}/

# 3. Trigger reload on Node 4
echo "[*] Notifying Node 4 FastAPI service..."
curl -X POST "http://node4:8000/reload?version=${VERSION}" || echo "Note: Node 4 server will load version on launch."
echo "[✓] Transfer complete!"
