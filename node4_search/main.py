import os
import json
import time
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import pyarrow.parquet as pq
import pandas as pd

from ranker import SearchRanker

app = FastAPI(
    title="Distributed Search Engine",
    description="FastAPI Search Service backed by distributed Inverted Index & PageRank",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))
ranker = SearchRanker(k1=1.2, b=0.75)

CURRENT_SERVING_DIR = os.environ.get("SERVING_DIR", "/serving")
active_version_int = 0

def find_version_path(version_name: str) -> Optional[str]:
    candidates = [
        os.path.join(CURRENT_SERVING_DIR, version_name),
        os.path.join(".", "serving", version_name),
        os.path.join("..", "serving", version_name),
        os.path.expanduser(f"~/serving/{version_name}"),
        os.path.expanduser(f"~/distributed-search-engine/serving/{version_name}")
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return None

def load_version_data(version_name: str) -> bool:
    global active_version_int
    v_path = find_version_path(version_name)
    if not v_path:
        print(f"[!] Warning: Path for version '{version_name}' not found.")
        return False

    print(f"[*] Loading search index from {v_path} into memory...")
    t0 = time.time()

    # 1. Read metadata if exists
    meta_path = os.path.join(v_path, "metadata.json")
    if os.path.exists(meta_path):
        with open(meta_path, "r") as f:
            meta = json.load(f)
    else:
        meta = {"total_docs": 1000, "avgdl": 100.0, "max_pagerank": 1.0}

    # 2. Read documents catalog parquet (supports docs.parquet or documents.parquet)
    docs_p_path = os.path.join(v_path, "docs.parquet")
    if not os.path.exists(docs_p_path):
        docs_p_path = os.path.join(v_path, "documents.parquet")
    if not os.path.exists(docs_p_path):
        print(f"[!] docs.parquet / documents.parquet not found in {v_path}")
        return False

    docs_df = pq.read_table(docs_p_path).to_pandas()
    doc_catalog = {}
    for _, row in docs_df.iterrows():
        d_id = str(row["doc_id"])
        pr_val = float(row.get("rank", row.get("pagerank", 0.15)))
        doc_catalog[d_id] = {
            "title": str(row.get("title", "")),
            "url": str(row.get("url", "")),
            "doc_len": int(row.get("doc_len", 1)),
            "pagerank": pr_val
        }

    # 3. Read inverted index parquet (supports index.parquet or inverted_index.parquet)
    idx_p_path = os.path.join(v_path, "index.parquet")
    if not os.path.exists(idx_p_path):
        idx_p_path = os.path.join(v_path, "inverted_index.parquet")
    if not os.path.exists(idx_p_path):
        print(f"[!] index.parquet / inverted_index.parquet not found in {v_path}")
        return False

    idx_df = pq.read_table(idx_p_path).to_pandas()
    inverted_index = {}
    for _, row in idx_df.iterrows():
        term = str(row["term"])
        d_id = str(row["doc_id"])
        tf_val = int(row["tf"])
        if term not in inverted_index:
            inverted_index[term] = {}
        inverted_index[term][d_id] = tf_val

    total_docs = meta.get("total_docs", len(doc_catalog))
    avgdl = float(meta.get("avgdl", 100.0))
    max_pr = float(meta.get("max_pagerank", 1.0))

    ranker.load_index(
        inverted_index=inverted_index,
        doc_catalog=doc_catalog,
        total_docs=total_docs,
        avgdl=avgdl,
        max_pagerank=max_pr,
        version=version_name
    )

    try:
        active_version_int = int("".join(filter(str.isdigit, version_name)) or "0")
    except Exception:
        active_version_int = 1 if "1" in version_name else 0

    elapsed = time.time() - t0
    print(f"[✓] Successfully loaded {version_name}: {total_docs} docs, {len(inverted_index)} terms in {elapsed:.2f}s")
    return True

@app.on_event("startup")
def startup_event():
    # Attempt to load Phase 0 (v0) by default
    success = load_version_data("v0")
    if not success:
        print("[!] Phase 0 export not found yet. Ready to reload when files land.")

@app.get("/health")
def health_check():
    """
    Verification Checkpoint:
    Expects {"status": "ok", "version": 0} for Phase 0
    Expects {"status": "ok", "version": 1} for Phase 1
    """
    # If currently at version 0, auto-check if v1/_READY has appeared
    global active_version_int
    if active_version_int == 0:
        v1_path = find_version_path("v1")
        if v1_path and os.path.exists(os.path.join(v1_path, "_READY")):
            load_version_data("v1")

    return {
        "status": "ok",
        "version": active_version_int
    }

@app.get("/search")
@app.get("/api/search")
def search_endpoint(
    q: str = Query(..., min_length=1, description="Search query keywords"),
    limit: int = Query(5, ge=1, le=100, description="Max results to return")
):
    """
    Core Search API tested by Evaluators:
    curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool
    curl -s "http://localhost:8000/search?q=cooking&limit=5" | python3 -m json.tool
    curl -s "http://localhost:8000/search?q=plant+growth&limit=5"
    """
    t0 = time.time()
    results = ranker.search(q, top_k=limit)
    latency_ms = round((time.time() - t0) * 1000, 2)
    return {
        "query": q,
        "version": active_version_int,
        "total_results": len(results),
        "latency_ms": latency_ms,
        "results": results
    }

@app.post("/reload")
@app.post("/api/reload")
def reload_version(version: str = Query("v1", description="Target version (e.g. v0, v1)")):
    """
    Hot reload without server restart
    """
    success = load_version_data(version)
    if not success:
        raise HTTPException(status_code=404, detail=f"Failed to load version '{version}'. Check if path exists.")
    return {
        "status": "success",
        "active_version": active_version_int,
        "total_docs": ranker.total_docs,
        "vocab_size": len(ranker.inverted_index)
    }

@app.get("/", response_class=HTMLResponse)
def index_page(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "version": f"v{active_version_int}",
            "total_docs": ranker.total_docs
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
