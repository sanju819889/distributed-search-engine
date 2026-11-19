#!/usr/bin/env bash
# Node 4: 100% Fresh Start & Clean Slate
# Author: Koushik (Search & UI Lead)

echo "=== [Node 4] Stopping existing search service & cleaning old cache ==="

# 1. Kill old uvicorn server
pkill -9 -f "uvicorn" || true
fuser -k 8000/tcp || true

sleep 1

# 2. Reset serving directory permissions
sudo mkdir -p /serving/v0 /serving/v1
sudo chmod -R 777 /serving

# 3. Start fresh server
echo "[*] Launching fresh FastAPI server..."
./start_server.sh
