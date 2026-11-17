#!/usr/bin/env python3
"""
Generates synthetic toy datasets (toy_pages.jsonl & toy_changes.jsonl)
matching the exact Wikipedia schema for rapid local testing.
"""

import os
import json

def generate_toy_data(output_dir="data"):
    os.makedirs(output_dir, exist_ok=True)
    
    # Phase 0: 5 connected pages with 1 duplicate and 1 dead link
    pages = [
        {
            "id": "doc1",
            "title": "Computer Science",
            "url": "https://simple.wikipedia.org/wiki/Computer_science",
            "text": "Computer science is the study of computation, automation, information, and algorithms. Computers process data rapidly.",
            "links": ["doc2", "doc3", "doc_does_not_exist"], # includes dead link
            "version": 1
        },
        {
            "id": "doc2",
            "title": "Algorithm",
            "url": "https://simple.wikipedia.org/wiki/Algorithm",
            "text": "An algorithm is a finite sequence of mathematically rigorous instructions used to solve class of problems or perform computation.",
            "links": ["doc1", "doc3"],
            "version": 1
        },
        {
            "id": "doc3",
            "title": "Search Engine",
            "url": "https://simple.wikipedia.org/wiki/Search_engine",
            "text": "A search engine is an information retrieval system designed to help find information stored on computer system like the World Wide Web.",
            "links": ["doc1", "doc4"],
            "version": 1
        },
        {
            "id": "doc4",
            "title": "PageRank",
            "url": "https://simple.wikipedia.org/wiki/PageRank",
            "text": "PageRank is an algorithm used by Google Search to rank web pages in search engine results. It measures the importance of website pages.",
            "links": ["doc3"],
            "version": 1
        },
        # Duplicate of doc1 with lower version (should be ignored by deduplication)
        {
            "id": "doc1",
            "title": "Old Computer Science",
            "url": "https://simple.wikipedia.org/wiki/Computer_science_old",
            "text": "Old outdated text.",
            "links": ["doc2"],
            "version": 0
        }
    ]

    p0_file = os.path.join(output_dir, "toy_pages.jsonl")
    with open(p0_file, "w") as f:
        for p in pages:
            f.write(json.dumps(p) + "\n")
    print(f"[✓] Created {p0_file}")

    # Phase 1: 1 updated page (higher version) and 1 brand new page
    changes = [
        {
            "id": "doc3",
            "title": "Distributed Search Engine",
            "url": "https://simple.wikipedia.org/wiki/Search_engine",
            "text": "A distributed search engine uses Apache Spark, HDFS, and Apache Iceberg to index and search millions of web documents with high throughput.",
            "links": ["doc1", "doc4", "doc5"],
            "version": 2
        },
        {
            "id": "doc5",
            "title": "Apache Spark",
            "url": "https://simple.wikipedia.org/wiki/Apache_Spark",
            "text": "Apache Spark is an open-source unified analytics engine for large-scale data processing and distributed machine learning.",
            "links": ["doc1", "doc3"],
            "version": 1
        }
    ]

    p1_file = os.path.join(output_dir, "toy_changes.jsonl")
    with open(p1_file, "w") as f:
        for c in changes:
            f.write(json.dumps(c) + "\n")
    print(f"[✓] Created {p1_file}")

if __name__ == "__main__":
    generate_toy_data()
