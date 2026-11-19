#!/usr/bin/env python3
"""
Generates synthetic toy datasets (toy_pages.jsonl & toy_changes.jsonl)
matching the exact real Wikipedia dataset schema:
- doc_id
- Phase 0: doc_id, url, title, text, links
- Phase 1: doc_id, op ('DELETE' or 'UPSERT'), title, url, text, links
"""

import os
import json

def generate_toy_data(output_dir="data"):
    os.makedirs(output_dir, exist_ok=True)
    
    # Phase 0: 5 connected pages with 1 duplicate and 1 dead link
    pages = [
        {
            "doc_id": "1",
            "title": "April",
            "url": "https://simple.wikipedia.org/wiki/April",
            "text": "April is the fourth month of the year in the Julian and Gregorian calendars. Spring flowers bloom.",
            "links": ["94", "146", "9999"] # 9999 is dead link
        },
        {
            "doc_id": "94",
            "title": "Beard",
            "url": "https://simple.wikipedia.org/wiki/Beard",
            "text": "A beard is the hair growing on the lower part of a man's face. Mustache and whiskers.",
            "links": ["1", "146"]
        },
        {
            "doc_id": "98",
            "title": "Black",
            "url": "https://simple.wikipedia.org/wiki/Black",
            "text": "Black is a color and shade. Absorbs all wavelengths of light.",
            "links": ["1"]
        },
        {
            "doc_id": "146",
            "title": "Cooking",
            "url": "https://simple.wikipedia.org/wiki/Cooking",
            "text": "Cooking is the art, technology, science and craft of preparing food for consumption with heat.",
            "links": ["1"]
        },
        # Duplicate of doc 1 (should be deduplicated)
        {
            "doc_id": "1",
            "title": "April",
            "url": "https://simple.wikipedia.org/wiki/April",
            "text": "April is the fourth month of the year.",
            "links": ["94"]
        }
    ]

    p0_file = os.path.join(output_dir, "toy_pages.jsonl")
    with open(p0_file, "w") as f:
        for p in pages:
            f.write(json.dumps(p) + "\n")
    print(f"[✓] Created {p0_file}")

    # Phase 1:
    # 1. Delete doc 1 ("April")
    # 2. Update doc 94 title to "Beard (Phase 1)"
    # 3. Add link to 146 from doc 98 and new doc phase1-new-001 so doc 146 gains rank
    changes = [
        {
            "doc_id": "1",
            "op": "DELETE"
        },
        {
            "doc_id": "94",
            "title": "Beard (Phase 1)",
            "url": "https://simple.wikipedia.org/wiki/Beard",
            "text": "A beard is hair on a man face. Updated in phase 1 with new information.",
            "links": ["146"],
            "op": "UPSERT"
        },
        {
            "doc_id": "98",
            "title": "Black (Phase 1)",
            "url": "https://simple.wikipedia.org/wiki/Black",
            "text": "Black is a color. Pigment absorbs light.",
            "links": ["146"],
            "op": "UPSERT"
        },
        {
            "doc_id": "phase1-new-001",
            "title": "Apache Spark Distributed Computing",
            "url": "https://simple.wikipedia.org/wiki/Apache_Spark",
            "text": "Apache Spark provides high speed distributed indexing and PageRank computation for search engines.",
            "links": ["94", "146"],
            "op": "UPSERT"
        }
    ]

    p1_file = os.path.join(output_dir, "toy_changes.jsonl")
    with open(p1_file, "w") as f:
        for c in changes:
            f.write(json.dumps(c) + "\n")
    print(f"[✓] Created {p1_file}")

if __name__ == "__main__":
    generate_toy_data()
