#!/usr/bin/env python3
"""
Full Pipeline Local Simulation & Verification
Tests the exact verification runbook logic:
1. Phase 0 Ingestion: Schema validation, deduplication, dead-link cleaning.
2. Phase 0 PageRank & Inverted Indexing.
3. Verification of baseline: doc 94 title is "Beard", doc 146 baseline rank.
4. Phase 1 Ingestion: Delete doc 1, update doc 94 to "Beard (Phase 1)", add new links to doc 146.
5. Phase 1 PageRank & Inverted Indexing: Doc 1 deleted (count is 0), doc 146 rank increased.
6. BM25 + PageRank Combined Ranking: k1=1.2, b=0.75, 0.7*BM25 + 0.3*PR.
"""

import os
import sys
import json
from collections import defaultdict

# Add parent path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "node4_search")))
from ranker import SearchRanker

def run_pagerank(documents, iterations=10, damping=0.85):
    ranks = {d_id: 1.0 for d_id in documents}
    for it in range(1, iterations + 1):
        new_ranks = {d_id: (1.0 - damping) for d_id in documents}
        for d_id, doc in documents.items():
            targets = [t for t in doc.get("links", []) if t in documents]
            if targets:
                share = (damping * ranks[d_id]) / len(targets)
                for t in targets:
                    new_ranks[t] += share
            else:
                share = (damping * ranks[d_id]) / max(1, len(documents))
                for other in documents:
                    new_ranks[other] += share
        ranks = new_ranks
    return ranks

def simulate_pipeline():
    print("==================================================")
    print(" DISTRIBUTED SEARCH ENGINE - VERIFICATION RUNBOOK")
    print("==================================================")

    from generate_toy_data import generate_toy_data
    generate_toy_data("data")

    # --- PHASE 0 ---
    print("\n--- [Phase 0] Ingestion ---")
    documents = {}
    with open("data/toy_pages.jsonl") as f:
        for line in f:
            rec = json.loads(line)
            d_id = rec["doc_id"]
            if d_id not in documents:
                documents[d_id] = rec

    print(f"[✓] Deduplicated to {len(documents)} unique documents.")
    # Clean dead links
    all_ids = set(documents.keys())
    for d_id, doc in documents.items():
        doc["links"] = [l for l in doc.get("links", []) if l in all_ids]

    ranks_p0 = run_pagerank(documents)
    print(f"[✓] Baseline Phase 0: doc 94 title = '{documents['94']['title']}'")
    print(f"[✓] Baseline Phase 0: doc 146 PageRank = {ranks_p0['146']:.4f}")

    # Build Ranker for Phase 0
    ranker = SearchRanker(k1=1.2, b=0.75)
    idx_p0 = defaultdict(dict)
    doc_lens_p0 = {}
    for d_id, doc in documents.items():
        toks = ranker.tokenize(doc["text"])
        doc_lens_p0[d_id] = max(1, len(toks))
        for t in toks:
            idx_p0[t][d_id] = idx_p0[t].get(d_id, 0) + 1

    catalog_p0 = {
        d_id: {
            "title": documents[d_id]["title"],
            "url": documents[d_id]["url"],
            "doc_len": doc_lens_p0[d_id],
            "pagerank": ranks_p0[d_id]
        }
        for d_id in documents
    }
    ranker.load_index(
        inverted_index=idx_p0,
        doc_catalog=catalog_p0,
        total_docs=len(documents),
        avgdl=sum(doc_lens_p0.values()) / len(doc_lens_p0),
        max_pagerank=max(ranks_p0.values()),
        version="v0"
    )

    search_beard_p0 = ranker.search("beard", top_k=1)
    print(f"[✓] Search 'beard' (Phase 0): Title = '{search_beard_p0[0]['title']}'")
    assert search_beard_p0[0]["title"] == "Beard", "Phase 0 title must be Beard"

    # --- PHASE 1 ---
    print("\n--- [Phase 1] Live Updates ---")
    with open("data/toy_changes.jsonl") as f:
        for line in f:
            rec = json.loads(line)
            d_id = rec["doc_id"]
            op = rec.get("op")
            if op == "DELETE":
                if d_id in documents:
                    del documents[d_id]
                    print(f"[*] Deleted doc_id '{d_id}' from dataset.")
            elif op == "UPSERT":
                documents[d_id] = rec
                print(f"[*] Upserted doc_id '{d_id}' into dataset.")

    # Clean dead links
    all_ids_p1 = set(documents.keys())
    for d_id, doc in documents.items():
        doc["links"] = [l for l in doc.get("links", []) if l in all_ids_p1]

    # Verification: doc 1 must be deleted
    assert "1" not in documents, "Doc 1 must be deleted in Phase 1!"
    print("[✓] Verification 2.3 Passed: doc_id '1' count is 0!")

    ranks_p1 = run_pagerank(documents)
    print(f"[✓] Phase 1: doc 94 title = '{documents['94']['title']}'")
    print(f"[✓] Phase 1: doc 146 PageRank = {ranks_p1['146']:.4f} (Baseline was {ranks_p0['146']:.4f})")
    assert ranks_p1["146"] > ranks_p0["146"], "Doc 146 must gain PageRank!"
    print("[✓] Verification 3.2 Passed: doc 146 gained rank!")

    # Build Ranker for Phase 1
    idx_p1 = defaultdict(dict)
    doc_lens_p1 = {}
    for d_id, doc in documents.items():
        toks = ranker.tokenize(doc["text"])
        doc_lens_p1[d_id] = max(1, len(toks))
        for t in toks:
            idx_p1[t][d_id] = idx_p1[t].get(d_id, 0) + 1

    catalog_p1 = {
        d_id: {
            "title": documents[d_id]["title"],
            "url": documents[d_id]["url"],
            "doc_len": doc_lens_p1[d_id],
            "pagerank": ranks_p1[d_id]
        }
        for d_id in documents
    }
    ranker.load_index(
        inverted_index=idx_p1,
        doc_catalog=catalog_p1,
        total_docs=len(documents),
        avgdl=sum(doc_lens_p1.values()) / len(doc_lens_p1),
        max_pagerank=max(ranks_p1.values()),
        version="v1"
    )

    search_beard_p1 = ranker.search("beard", top_k=1)
    print(f"[✓] Search 'beard' (Phase 1): Title = '{search_beard_p1[0]['title']}'")
    assert search_beard_p1[0]["title"] == "Beard (Phase 1)", "Phase 1 title must be 'Beard (Phase 1)'"
    print("[✓] Verification 3.1 Passed: doc 94 updated in place to 'Beard (Phase 1)'!")

    # Multi-word search
    multi_res = ranker.search("distributed spark", top_k=2)
    assert len(multi_res) > 0, "Multi-word search must return results"
    print(f"[✓] Multi-word query 'distributed spark': {len(multi_res)} results returned successfully.")

    print("\n==================================================")
    print(" [✓] ALL EVALUATION CHECKS PASSED PERFECTLY!")
    print("==================================================")

if __name__ == "__main__":
    simulate_pipeline()
