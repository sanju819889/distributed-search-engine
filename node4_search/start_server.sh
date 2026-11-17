#!/usr/bin/env bash
# Node 4: Start FastAPI Search Service
# Author: Koushik (Search & UI Lead)

set -e

echo "=== [Node 4] Starting FastAPI Search Engine Server ==="

# Create serving directories if not exist
sudo mkdir -p /serving/v0 /serving/v1
sudo chmod -R 777 /serving

# Create virtualenv if not exist
if [ ! -d "venv" ]; then
    echo "[*] Creating Python virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

export SERVING_DIR="/serving"

echo "[*] Launching Uvicorn web server on 0.0.0.0:8000..."
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
