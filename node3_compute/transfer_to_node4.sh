#!/usr/bin/env bash
# Node 3: Transfer exported Parquet files to Node 4 (Serving Node)
# Usage: ./transfer_to_node4.sh [v0|v1]

VERSION=${1:-v0}
EXPORT_DIR="/tmp/search_export/${VERSION}"

if [ ! -d "$EXPORT_DIR" ]; then
    echo "[!] Error: Export directory $EXPORT_DIR does not exist! Run compute_pipeline.py first."
    exit 1
fi

echo "[*] Transferring ${VERSION} export to Node 4 (/serving/${VERSION})..."
ssh node4 "mkdir -p /serving/${VERSION}"
scp -r ${EXPORT_DIR}/* node4:/serving/${VERSION}/

echo "[✓] Files transferred to Node 4!"
echo "[*] Triggering Node 4 reload API..."
curl -X POST "http://node4:8000/api/reload?version=${VERSION}" || echo "Node 4 server not running or will pick it up on startup."
