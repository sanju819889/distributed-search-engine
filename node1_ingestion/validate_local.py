#!/usr/bin/env python3
"""
Node 1: Standalone Local Data Quality & Validation Checker
Author: Sanjeev (Node 1 Lead)

Validates both Phase 0 (pages.jsonl) and Phase 1 (changes.json / changes.jsonl).
Checks:
- Malformed JSON records
- Missing required fields (doc_id, title, text for documents; doc_id, op for changes)
- Deduplication by doc_id
- Dead / Dangling link detection against valid documents
"""

import sys
import json
from collections import defaultdict

def validate_dataset(filepath):
    print(f"[*] Validating {filepath}...")
    total_lines = 0
    malformed_json = 0
    missing_fields = 0
    delete_ops = 0
    upsert_ops = 0

    records_by_id = {}
    all_links = []

    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            total_lines += 1
            try:
                rec = json.loads(line)
            except Exception:
                malformed_json += 1
                continue

            if not isinstance(rec, dict):
                malformed_json += 1
                continue

            # Support doc_id (primary) or id (fallback)
            doc_id = str(rec.get("doc_id") or rec.get("id") or "").strip()
            if not doc_id:
                missing_fields += 1
                continue

            op = rec.get("op")
            if op == "DELETE":
                delete_ops += 1
                continue
            elif op == "UPSERT":
                upsert_ops += 1

            # Validate document record (Phase 0 page or Phase 1 UPSERT)
            title = rec.get("title")
            text = rec.get("text")
            if not title or not text:
                missing_fields += 1
                continue

            links = rec.get("links", [])
            if not isinstance(links, list):
                links = []

            records_by_id[doc_id] = {
                "doc_id": doc_id,
                "title": title,
                "url": rec.get("url", ""),
                "links": [str(l) for l in links]
            }

    unique_doc_ids = set(records_by_id.keys())
    total_valid_docs = len(records_by_id)

    # Dead link detection
    total_links = 0
    dead_links = 0
    for doc in records_by_id.values():
        for target in doc["links"]:
            total_links += 1
            if target not in unique_doc_ids:
                dead_links += 1

    print("\n--- Data Quality Report ---")
    print(f"File parsed:               {filepath}")
    print(f"Total lines parsed:        {total_lines}")
    print(f"Malformed JSON records:     {malformed_json}")
    print(f"Records missing fields:     {missing_fields}")
    if delete_ops > 0 or upsert_ops > 0:
        print(f"DELETE operations:         {delete_ops}")
        print(f"UPSERT operations:         {upsert_ops}")
    print(f"Total valid document records: {total_valid_docs}")
    print(f"Unique Document IDs:       {len(unique_doc_ids)}")
    print(f"Total outgoing links:      {total_links}")
    print(f"Dead / Dangling links:     {dead_links} (will be pruned during ingestion)")
    print("---------------------------\n")

    return total_valid_docs > 0 or delete_ops > 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 validate_local.py <path_to_jsonl_or_json>")
        sys.exit(1)
    validate_dataset(sys.argv[1])
