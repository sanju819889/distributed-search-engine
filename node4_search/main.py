import os
import json
import time
from typing import Optional
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

def load_version_data(version_name: str) -> bool:
    v_path = os.path.join(CURRENT_SERVING_DIR, version_name)
    if not os.path.exists(v_path):
        print(f"[!] Warning: Path {v_path} does not exist.")
        return False

    print(f"[*] Loading search index from {v_path} into memory...")
    t0 = time.time()

    # 1. Read metadata
    meta_path = os.path.join(v_path, "metadata.json")
    if os.path.exists(meta_path):
        with open(meta_path, "r") as f:
            meta = json.load(f)
    else:
        meta = {"total_docs": 1000, "avgdl": 100.0, "max_pagerank": 1.0}

    # 2. Read documents catalog parquet
    docs_p_path = os.path.join(v_path, "documents.parquet")
    if not os.path.exists(docs_p_path):
        print(f"[!] documents.parquet not found in {v_path}")
        return False
    docs_df = pq.read_table(docs_p_path).to_pandas()
    doc_catalog = {}
    for _, row in docs_df.iterrows():
        doc_catalog[str(row["doc_id"])] = {
            "title": str(row.get("title", "")),
            "url": str(row.get("url", "")),
            "doc_len": int(row.get("doc_len", 1)),
            "pagerank": float(row.get("pagerank", 0.15))
        }

    # 3. Read inverted index parquet
    idx_p_path = os.path.join(v_path, "inverted_index.parquet")
    if not os.path.exists(idx_p_path):
        print(f"[!] inverted_index.parquet not found in {v_path}")
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

    elapsed = time.time() - t0
    print(f"[✓] Successfully loaded {version_name}: {total_docs} docs, {len(inverted_index)} terms in {elapsed:.2f}s")
    return True

@app.on_event("startup")
def startup_event():
    # Attempt to load Phase 0 (v0) by default
    success = load_version_data("v0")
    if not success:
        print("[!] Phase 0 export not yet present. Waiting for transfer or /api/reload call.")

@app.get("/", response_class=HTMLResponse)
def index_page(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "version": ranker.version,
            "total_docs": ranker.total_docs
        }
    )

@app.get("/api/search")
def search_api(q: str = Query(..., min_length=1, description="Search query keywords")):
    t0 = time.time()
    results = ranker.search(q, top_k=20)
    latency_ms = round((time.time() - t0) * 1000, 2)
    return {
        "query": q,
        "version": ranker.version,
        "total_results": len(results),
        "latency_ms": latency_ms,
        "results": results
    }

@app.post("/api/reload")
def reload_version(version: str = Query(..., description="Target version folder name (e.g. v0, v1)")):
    success = load_version_data(version)
    if not success:
        raise HTTPException(status_code=404, detail=f"Failed to load version '{version}'. Check if path exists.")
    return {
        "status": "success",
        "active_version": ranker.version,
        "total_docs": ranker.total_docs,
        "vocab_size": len(ranker.inverted_index)
    }

@app.get("/api/status")
def status_api():
    return {
        "status": "online",
        "active_version": ranker.version,
        "total_docs": ranker.total_docs,
        "vocab_size": len(ranker.inverted_index),
        "avgdl": ranker.avgdl,
        "max_pagerank": ranker.max_pagerank
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
