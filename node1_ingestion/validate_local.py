#!/usr/bin/env python3
"""
Node 1: Standalone Local Data Quality & Validation Checker
Author: Sanjeev (Node 1 Lead)

Usage:
  python3 validate_local.py ../data/pages.jsonl
"""

import sys
import json
from collections import defaultdict

def validate_dataset(filepath):
    print(f"[*] Validating {filepath}...")
    total_lines = 0
    malformed_json = 0
    missing_fields = 0
    records_by_id = defaultdict(list)

    required_keys = {"id", "title", "url", "text", "links", "version"}

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

            # Check required fields
            if not isinstance(rec, dict) or not required_keys.issubset(rec.keys()):
                missing_fields += 1
                continue

            if not rec.get("id") or not rec.get("text") or rec.get("version") is None:
                missing_fields += 1
                continue

            records_by_id[rec["id"]].append(rec)

    total_valid = sum(len(v) for v in records_by_id.values())
    unique_ids = len(records_by_id)
    duplicates = total_valid - unique_ids

    # Check dead links
    all_valid_ids = set(records_by_id.keys())
    dead_links_count = 0
    total_links_count = 0

    for doc_id, versions in records_by_id.items():
        # Get highest version
        latest = max(versions, key=lambda x: x.get("version", 0))
        links = latest.get("links", [])
        if isinstance(links, list):
            for target in links:
                total_links_count += 1
                if target not in all_valid_ids:
                    dead_links_count += 1

    print("\n--- Data Quality Report ---")
    print(f"Total lines parsed:        {total_lines}")
    print(f"Malformed JSON records:     {malformed_json}")
    print(f"Records missing fields:     {missing_fields}")
    print(f"Total valid records:        {total_valid}")
    print(f"Unique Document IDs:        {unique_ids}")
    print(f"Duplicate documents:        {duplicates} (will be deduplicated by highest version)")
    print(f"Total outgoing links:       {total_links_count}")
    print(f"Dead / Dangling links:      {dead_links_count} (will be removed during ingestion)")
    print("---------------------------\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 validate_local.py <path_to_jsonl>")
        sys.exit(1)
    validate_dataset(sys.argv[1])
