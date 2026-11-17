#!/usr/bin/env python3
"""
Node 3: Distributed Compute Pipeline (Indexing + PageRank + Parquet Export)
Author: Abhishek (Compute Lead)

Deliverables:
1. Distributed Inverted Indexing from Iceberg table 'lakehouse.default.documents'
2. Distributed PageRank (minimum 10 iterations, damping factor 0.85, RDD checkpointing)
3. Plain Parquet export (index, docs, corpus metadata) for serving on Node 4
"""

import sys
import os
import re
import json
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, split, lower, regexp_replace, length,
    count, avg, max as spark_max, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

STOPWORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
    "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
    "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
    "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
    "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
    "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
    "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
    "they've", "this", "those", "through", "to", "too", "under", "until", "up",
    "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
    "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
    "yourself", "yourselves"
}

def create_spark_session(master_url=None, warehouse_uri="hdfs://node2:9000/iceberg"):
    builder = (
        SparkSession.builder
        .appName("Node3-Compute-Indexing-PageRank")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse_uri)
    )
    if master_url:
        builder = builder.master(master_url)
    return builder.getOrCreate()

def compute_pagerank(sc, links_rdd, all_doc_ids, iterations=10, damping=0.85, checkpoint_dir="hdfs://node2:9000/checkpoints"):
    """
    Computes PageRank with RDD checkpointing to prevent StackOverflowError
    """
    print(f"[*] Starting Distributed PageRank ({iterations} iterations, damping={damping})...")
    sc.setCheckpointDir(checkpoint_dir)

    # Initialize ranks: doc_id -> 1.0
    # links_rdd: (doc_id, [target_doc_ids])
    ranks = sc.parallelize(all_doc_ids).map(lambda doc: (doc, 1.0))

    for it in range(1, iterations + 1):
        # (doc, (neighbors, rank))
        contribs = links_rdd.join(ranks).flatMap(
            lambda x: [(dest, x[1][1] / len(x[1][0])) for dest in x[1][0]] if x[1][0] else []
        )
        
        # New rank = (1 - damping) + damping * sum(contribs)
        # Full union with all_doc_ids ensures docs with no incoming links still get base score
        base_contribs = sc.parallelize(all_doc_ids).map(lambda doc: (doc, 0.0))
        all_contribs = contribs.union(base_contribs).reduceByKey(lambda a, b: a + b)
        
        ranks = all_contribs.mapValues(lambda score: (1.0 - damping) + damping * score)

        # Checkpoint every 2 iterations to truncate RDD DAG lineage
        if it % 2 == 0:
            ranks.checkpoint()
            print(f"    - Completed iteration {it}/{iterations} (lineage checkpointed)")
        else:
            print(f"    - Completed iteration {it}/{iterations}")

    return ranks

def run_compute(version="v0", master_url=None, warehouse_uri="hdfs://node2:9000/iceberg", export_dir="/tmp/export"):
    spark = create_spark_session(master_url, warehouse_uri)
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    version_export_path = os.path.join(export_dir, version)
    os.makedirs(version_export_path, exist_ok=True)

    print("==================================================")
    print(f"[*] Node 3 Compute Pipeline Running (Version: {version})")
    print(f"[*] Iceberg Source Table: lakehouse.default.documents")
    print(f"[*] Target Export Path: {version_export_path}")
    print("==================================================")

    # 1. Read validated documents from Apache Iceberg
    docs_df = spark.sql("SELECT id, title, url, text, links FROM lakehouse.default.documents")
    total_docs = docs_df.count()
    print(f"[*] Total documents loaded from Iceberg: {total_docs}")

    if total_docs == 0:
        print("[!] Error: Iceberg table is empty! Did Node 1 run ingestion?")
        sys.exit(1)

    all_doc_ids = [row["id"] for row in docs_df.select("id").collect()]

    # 2. PageRank Computation
    links_data = [(row["id"], row["links"] if row["links"] else []) for row in docs_df.select("id", "links").collect()]
    links_rdd = sc.parallelize(links_data)
    
    ranks_rdd = compute_pagerank(
        sc, links_rdd, all_doc_ids, iterations=10, damping=0.85
    )

    ranks_df = ranks_rdd.toDF(["doc_id", "pagerank"])

    # 3. Distributed Inverted Indexing
    print("[*] Computing distributed Inverted Index and Term Frequencies...")

    # Tokenize: lowercase, replace non-alphanumeric with spaces, split into terms
    clean_text_df = docs_df.withColumn(
        "clean_text",
        regexp_replace(lower(col("text")), r"[^a-z0-9\s]", " ")
    )

    words_df = clean_text_df.select(
        col("id").alias("doc_id"),
        explode(split(col("clean_text"), r"\s+")).alias("term")
    ).filter(
        (length(col("term")) >= 2) & (~col("term").isin(list(STOPWORDS)))
    )

    # Compute Term Frequency (TF) per document
    tf_df = words_df.groupBy("term", "doc_id").agg(count("*").alias("tf"))

    # Compute Document Length len(D) = total tokens in document
    doc_lens_df = words_df.groupBy("doc_id").agg(count("*").alias("doc_len"))

    # Join metadata with document lengths and PageRank scores
    enriched_docs = (
        docs_df.select(col("id").alias("doc_id"), "title", "url")
        .join(doc_lens_df, on="doc_id", how="left")
        .fillna({"doc_len": 1})
        .join(ranks_df, on="doc_id", how="left")
        .fillna({"pagerank": 0.15})
    )

    # Calculate average document length across corpus
    avgdl_val = enriched_docs.select(avg("doc_len")).collect()[0][0] or 1.0
    max_pr_val = enriched_docs.select(spark_max("pagerank")).collect()[0][0] or 1.0

    print(f"[*] Corpus Stats: Total Docs = {total_docs}, avgdl = {avgdl_val:.2f}, max(PageRank) = {max_pr_val:.4f}")

    # 4. Plain Parquet Export for Serving Node (Node 4)
    print(f"[*] Exporting plain Parquet files to {version_export_path}...")
    
    # Write inverted index
    tf_df.coalesce(2).write.mode("overwrite").parquet(os.path.join(version_export_path, "inverted_index.parquet"))
    
    # Write enriched document catalog
    enriched_docs.coalesce(1).write.mode("overwrite").parquet(os.path.join(version_export_path, "documents.parquet"))

    # Write summary metadata
    meta = {
        "version": version,
        "total_docs": total_docs,
        "avgdl": float(avgdl_val),
        "max_pagerank": float(max_pr_val)
    }
    with open(os.path.join(version_export_path, "metadata.json"), "w") as f:
        json.dump(meta, f, indent=2)

    print(f"[✓] Export finished successfully in {version_export_path}!")
    print(f"==================================================")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Node 3 Compute Pipeline")
    parser.add_argument("--version", default="v0", help="Version identifier (e.g. v0, v1)")
    parser.add_argument("--master", default=None, help="Spark master URL (e.g. spark://node3:7077)")
    parser.add_argument("--warehouse", default="hdfs://node2:9000/iceberg", help="Iceberg warehouse URI")
    parser.add_argument("--export_dir", default="/tmp/search_export", help="Target export directory")
    args = parser.parse_args()

    run_compute(args.version, args.master, args.warehouse, args.export_dir)
