#!/usr/bin/env python3
"""
Node 1: Ingestion & Data Quality Pipeline
Author: Sanjeev (Node 1 Lead)

Requirements Satisfied:
1. Parse and validate input records from JSON / JSONL.
2. Reject malformed records or missing required fields (doc_id, title, text).
3. Deduplicate by doc_id.
4. Remove dead/dangling links that point to documents not in the valid corpus.
5. Idempotent Apache Iceberg operations:
   - Phase 0: MERGE INTO iceberg_catalog.lake.pages
   - Phase 1: DELETE (doc_id) and MERGE INTO (upserts)
   - Re-running the pipeline is completely idempotent (no duplicates or corrupted state).
"""

import sys
import os

# Ensure PySpark workers and driver use the exact same Python binary
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, coalesce, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType
)

def create_spark_session(master_url=None, warehouse_uri="hdfs://node2:9000/iceberg"):
    builder = (
        SparkSession.builder
        .appName("Node1-Ingestion-Pipeline")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
        .config("spark.sql.catalog.iceberg_catalog.warehouse", warehouse_uri)
    )
    if master_url:
        builder = builder.master(master_url)
    return builder.getOrCreate()

def run_ingestion(input_path, phase=None, master_url=None, warehouse_uri="hdfs://node2:9000/iceberg"):
    print(f"==================================================")
    print(f"[*] Starting Ingestion Pipeline: Input='{input_path}'")
    print(f"[*] Warehouse URI: {warehouse_uri}")
    print(f"==================================================")

    spark = create_spark_session(master_url, warehouse_uri)
    spark.sparkContext.setLogLevel("WARN")

    # Ensure Iceberg namespace and table exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.lake")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg_catalog.lake.pages (
            doc_id STRING,
            title STRING,
            url STRING,
            text STRING,
            links ARRAY<STRING>
        )
        USING iceberg
    """)

    # 1. Flexible Schema to support doc_id (primary), id (fallback), and op (for Phase 1 changes)
    input_schema = StructType([
        StructField("doc_id", StringType(), True),
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("url", StringType(), True),
        StructField("text", StringType(), True),
        StructField("links", ArrayType(StringType()), True),
        StructField("op", StringType(), True)
    ])

    raw_df = spark.read.schema(input_schema).json(input_path)
    # Normalize doc_id
    normalized_df = raw_df.withColumn(
        "doc_id",
        coalesce(col("doc_id"), col("id"))
    )

    total_records = normalized_df.count()
    print(f"[*] Read {total_records} raw records from {input_path}")

    # Determine phase if not explicitly provided
    has_op = "op" in raw_df.columns and normalized_df.filter(col("op").isNotNull()).count() > 0
    if not phase:
        phase = "phase1" if has_op else "phase0"

    print(f"[*] Executing pipeline for: {phase.upper()}")

    # ==========================================
    # PHASE 1 HANDLING (Deletes + Upserts)
    # ==========================================
    if phase.lower() == "phase1" or has_op:
        # Separate DELETEs and UPSERTs
        deletes_df = normalized_df.filter(
            col("op") == "DELETE"
        ).filter(col("doc_id").isNotNull() & (col("doc_id") != ""))

        delete_ids = [r[0] for r in deletes_df.select("doc_id").distinct().collect() if r[0]]
        print(f"[*] Found {len(delete_ids)} DELETE operations.")

        if delete_ids:
            print(f"[*] Applying deletions to 'iceberg_catalog.lake.pages' (e.g., doc_id '1')...")
            deletes_df.select("doc_id").distinct().createOrReplaceTempView("pending_deletes")
            spark.sql("""
                DELETE FROM iceberg_catalog.lake.pages
                WHERE doc_id IN (SELECT doc_id FROM pending_deletes)
            """)
            print("[✓] Deletions applied successfully.")

        # Process UPSERTs
        upserts_df = normalized_df.filter(
            (col("op") == "UPSERT") | col("op").isNull()
        )
        valid_upserts = upserts_df.filter(
            col("doc_id").isNotNull() & (col("doc_id") != "") &
            col("title").isNotNull() & (col("title") != "") &
            col("text").isNotNull() & (col("text") != "")
        ).dropDuplicates(["doc_id"])

        print(f"[*] Valid UPSERT records: {valid_upserts.count()}")

        # Clean dangling links against existing docs in Iceberg + incoming upsert IDs
        existing_doc_ids = set(
            r[0] for r in spark.sql("SELECT doc_id FROM iceberg_catalog.lake.pages").collect() if r[0]
        )
        new_doc_ids = set(r[0] for r in valid_upserts.select("doc_id").collect() if r[0])
        valid_target_ids = existing_doc_ids.union(new_doc_ids)

        def clean_links(links_arr):
            if not links_arr:
                return []
            return [str(l) for l in links_arr if str(l) in valid_target_ids]

        clean_links_udf = udf(clean_links, ArrayType(StringType()))
        cleaned_upserts = valid_upserts.withColumn("links", clean_links_udf(col("links")))

        # Idempotent MERGE INTO
        cleaned_upserts.select("doc_id", "title", "url", "text", "links").createOrReplaceTempView("batch_upserts")
        merge_query = """
            MERGE INTO iceberg_catalog.lake.pages target
            USING batch_upserts source
            ON target.doc_id = source.doc_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.title = source.title,
                    target.url = source.url,
                    target.text = source.text,
                    target.links = source.links
            WHEN NOT MATCHED THEN
                INSERT (doc_id, title, url, text, links)
                VALUES (source.doc_id, source.title, source.url, source.text, source.links)
        """
        print("[*] Merging Phase 1 upserts into 'iceberg_catalog.lake.pages'...")
        spark.sql(merge_query)

    # ==========================================
    # PHASE 0 HANDLING (Initial Ingestion)
    # ==========================================
    else:
        # Filter valid records: missing title or text or empty doc_id rejected
        valid_df = normalized_df.filter(
            col("doc_id").isNotNull() & (col("doc_id") != "") &
            col("title").isNotNull() & (col("title") != "") &
            col("text").isNotNull() & (col("text") != "")
        )
        valid_count = valid_df.count()
        print(f"[*] Valid document records: {valid_count} (Rejected: {total_records - valid_count})")

        # Deduplicate by doc_id
        deduped_df = valid_df.dropDuplicates(["doc_id"])
        print(f"[*] Unique documents after deduplication: {deduped_df.count()}")

        # Dead link pruning (pure driver list, no RDD conversion)
        valid_doc_ids = set(r[0] for r in deduped_df.select("doc_id").collect() if r[0])

        def clean_links_p0(links_arr):
            if not links_arr:
                return []
            return [str(l) for l in links_arr if str(l) in valid_doc_ids]

        clean_links_udf = udf(clean_links_p0, ArrayType(StringType()))
        cleaned_df = deduped_df.withColumn("links", clean_links_udf(col("links")))

        # Idempotent MERGE INTO
        cleaned_df.select("doc_id", "title", "url", "text", "links").createOrReplaceTempView("phase0_docs")
        merge_query = """
            MERGE INTO iceberg_catalog.lake.pages target
            USING phase0_docs source
            ON target.doc_id = source.doc_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.title = source.title,
                    target.url = source.url,
                    target.text = source.text,
                    target.links = source.links
            WHEN NOT MATCHED THEN
                INSERT (doc_id, title, url, text, links)
                VALUES (source.doc_id, source.title, source.url, source.text, source.links)
        """
        print("[*] Merging Phase 0 pages into 'iceberg_catalog.lake.pages'...")
        spark.sql(merge_query)

    # ==========================================
    # VERIFICATION & REPORTING
    # ==========================================
    total_in_table = spark.sql("SELECT COUNT(*) AS count FROM iceberg_catalog.lake.pages").collect()[0]["count"]
    print(f"\n[✓] Ingestion Successful!")
    print(f"[✓] Total active documents in 'iceberg_catalog.lake.pages': {total_in_table}")

    print("\n--- Iceberg Commit History (Snapshots) ---")
    try:
        history_df = spark.sql("SELECT made_current_at, snapshot_id FROM iceberg_catalog.lake.pages.history ORDER BY made_current_at DESC LIMIT 5")
        history_df.show(truncate=False)
    except Exception:
        spark.sql("SELECT * FROM iceberg_catalog.lake.pages.history LIMIT 5").show(truncate=False)

    print("==================================================")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Node 1 Ingestion Pipeline")
    parser.add_argument("input_path", help="Path to input jsonl file (e.g. data/pages.jsonl or data/changes.json)")
    parser.add_argument("--phase", default=None, help="Phase override: 'phase0' or 'phase1'")
    parser.add_argument("--master", default=None, help="Spark master URL (e.g. spark://node3:7077)")
    parser.add_argument("--warehouse", default="hdfs://node2:9000/iceberg", help="Iceberg warehouse URI")
    args = parser.parse_args()

    run_ingestion(args.input_path, args.phase, args.master, args.warehouse)
