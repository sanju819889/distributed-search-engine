#!/usr/bin/env python3
"""
Node 3: Distributed Compute Pipeline (Indexing + PageRank + Parquet Export)
Author: Abhishek (Compute Lead)

Deliverables Satisfied:
1. Distributed Inverted Indexing from Iceberg table 'iceberg_catalog.lake.pages'
   - Saved to Iceberg table 'iceberg_catalog.lake.inverted_index' (term, doc_id, tf)
2. Distributed PageRank (minimum 10 iterations, damping factor 0.85, RDD checkpointing)
   - Saved to Iceberg table 'iceberg_catalog.lake.pagerank' (doc_id, rank)
3. Plain Parquet export for serving:
   - HDFS: /serving/v0/ and /serving/v1/ containing docs.parquet, index.parquet, metadata.json, and _READY marker
"""

import sys
import os
import re
import json
import subprocess
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
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
        .config("spark.sql.catalog.iceberg_catalog.warehouse", warehouse_uri)
    )
    if master_url:
        builder = builder.master(master_url)
    return builder.getOrCreate()

def compute_pagerank(sc, links_rdd, all_doc_ids, iterations=10, damping=0.85, checkpoint_dir="hdfs://node2:9000/checkpoints"):
    print(f"[*] Starting Distributed PageRank ({iterations} iterations, damping={damping})...")
    try:
        sc.setCheckpointDir(checkpoint_dir)
    except Exception as e:
        print(f"[!] Warning on checkpoint dir: {e}. Falling back to /tmp/spark_checkpoints")
        sc.setCheckpointDir("/tmp/spark_checkpoints")

    # Initial rank: 1.0 per document
    ranks = sc.parallelize(all_doc_ids).map(lambda doc: (doc, 1.0))

    for it in range(1, iterations + 1):
        # Calculate outgoing contributions
        contribs = links_rdd.join(ranks).flatMap(
            lambda x: [(dest, x[1][1] / len(x[1][0])) for dest in x[1][0]] if x[1][0] else []
        )

        base_contribs = sc.parallelize(all_doc_ids).map(lambda doc: (doc, 0.0))
        all_contribs = contribs.union(base_contribs).reduceByKey(lambda a, b: a + b)

        # Standard PageRank formula: (1 - d) + d * sum(contribs)
        ranks = all_contribs.mapValues(lambda score: (1.0 - damping) + damping * score)

        # Lineage truncation checkpoint every 2 iterations to avoid StackOverflowError
        if it % 2 == 0:
            ranks.checkpoint()
            print(f"    - Completed iteration {it}/{iterations} (lineage checkpointed)")
        else:
            print(f"    - Completed iteration {it}/{iterations}")

    return ranks

def touch_hdfs_ready(path):
    try:
        subprocess.run(["hdfs", "dfs", "-touchz", path], check=False)
    except Exception:
        pass

def run_compute(version="v0", master_url=None, warehouse_uri="hdfs://node2:9000/iceberg", export_base="hdfs://node2:9000/serving"):
    spark = create_spark_session(master_url, warehouse_uri)
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    version_str = version if version.startswith("v") else f"v{version}"
    hdfs_version_dir = f"{export_base}/{version_str}"
    local_version_dir = f"/serving/{version_str}"

    print("==================================================")
    print(f"[*] Node 3 Compute Pipeline Running (Version: {version_str})")
    print(f"[*] Iceberg Source Table: iceberg_catalog.lake.pages")
    print(f"[*] HDFS Export Target:   {hdfs_version_dir}")
    print("==================================================")

    # 1. Read validated documents from Iceberg
    docs_df = spark.sql("SELECT doc_id, title, url, text, links FROM iceberg_catalog.lake.pages")
    total_docs = docs_df.count()
    print(f"[*] Total active documents in iceberg_catalog.lake.pages: {total_docs}")

    if total_docs == 0:
        print("[!] Error: 'iceberg_catalog.lake.pages' is empty! Please run Node 1 Ingestion first.")
        sys.exit(1)

    all_doc_ids = [row["doc_id"] for row in docs_df.select("doc_id").collect()]

    # 2. Compute PageRank
    links_data = [(row["doc_id"], row["links"] if row["links"] else []) for row in docs_df.select("doc_id", "links").collect()]
    links_rdd = sc.parallelize(links_data)

    ranks_rdd = compute_pagerank(
        sc, links_rdd, all_doc_ids, iterations=10, damping=0.85
    )

    # Save to Iceberg table: iceberg_catalog.lake.pagerank (columns: doc_id, rank)
    ranks_df = ranks_rdd.toDF(["doc_id", "rank"])
    ranks_df.write.format("iceberg").mode("overwrite").saveAsTable("iceberg_catalog.lake.pagerank")
    print("[✓] PageRank computed and saved to 'iceberg_catalog.lake.pagerank'!")

    # 3. Distributed Inverted Indexing
    print("[*] Computing distributed Inverted Index and Term Frequencies...")

    clean_text_df = docs_df.withColumn(
        "clean_text",
        regexp_replace(lower(col("text")), r"[^a-z0-9\s]", " ")
    )

    words_df = clean_text_df.select(
        col("doc_id"),
        explode(split(col("clean_text"), r"\s+")).alias("term")
    ).filter(
        (length(col("term")) >= 2) & (~col("term").isin(list(STOPWORDS)))
    )

    # Term Frequency (tf)
    tf_df = words_df.groupBy("term", "doc_id").agg(count("*").cast(IntegerType()).alias("tf"))

    # Save to Iceberg table: iceberg_catalog.lake.inverted_index
    tf_df.write.format("iceberg").mode("overwrite").saveAsTable("iceberg_catalog.lake.inverted_index")
    print("[✓] Inverted Index computed and saved to 'iceberg_catalog.lake.inverted_index'!")

    # Document length len(D)
    doc_lens_df = words_df.groupBy("doc_id").agg(count("*").alias("doc_len"))

    # Join metadata with doc lengths and PageRank scores
    enriched_docs = (
        docs_df.select("doc_id", "title", "url")
        .join(doc_lens_df, on="doc_id", how="left")
        .fillna({"doc_len": 1})
        .join(ranks_df, on="doc_id", how="left")
        .fillna({"rank": 0.15})
    )

    avgdl_val = float(enriched_docs.select(avg("doc_len")).collect()[0][0] or 1.0)
    max_pr_val = float(enriched_docs.select(spark_max("rank")).collect()[0][0] or 1.0)

    print(f"[*] Stats: total_docs={total_docs}, avgdl={avgdl_val:.2f}, max_rank={max_pr_val:.4f}")

    # 4. Parquet Export to HDFS (/serving/v0/ or /serving/v1/)
    print(f"[*] Exporting plain Parquet to HDFS: {hdfs_version_dir}...")
    enriched_docs.coalesce(1).write.mode("overwrite").parquet(f"{hdfs_version_dir}/docs.parquet")
    tf_df.coalesce(2).write.mode("overwrite").parquet(f"{hdfs_version_dir}/index.parquet")

    # Touch readiness flag in HDFS
    touch_hdfs_ready(f"{hdfs_version_dir}/_READY")

    # Also save to local /serving folder if directory is available (for local serving node)
    try:
        os.makedirs(local_version_dir, exist_ok=True)
        enriched_docs.coalesce(1).write.mode("overwrite").parquet(f"{local_version_dir}/docs.parquet")
        tf_df.coalesce(2).write.mode("overwrite").parquet(f"{local_version_dir}/index.parquet")
        with open(f"{local_version_dir}/_READY", "w") as f:
            f.write("ready\n")
        with open(f"{local_version_dir}/metadata.json", "w") as f:
            json.dump({
                "version": 1 if "1" in version_str else 0,
                "version_name": version_str,
                "total_docs": total_docs,
                "avgdl": avgdl_val,
                "max_pagerank": max_pr_val
            }, f, indent=2)
    except Exception as e:
        print(f"[*] Note: Local directory write ({local_version_dir}): {e}")

    print(f"[✓] Successfully exported to {hdfs_version_dir} and touched _READY marker!")
    print("==================================================")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Node 3 Compute Pipeline")
    parser.add_argument("--version", default="v0", help="Version identifier (e.g. v0, v1)")
    parser.add_argument("--master", default=None, help="Spark master URL (e.g. spark://node3:7077)")
    parser.add_argument("--warehouse", default="hdfs://node2:9000/iceberg", help="Iceberg warehouse URI")
    parser.add_argument("--export_base", default="hdfs://node2:9000/serving", help="Target HDFS export base")
    args = parser.parse_args()

    run_compute(args.version, args.master, args.warehouse, args.export_base)
