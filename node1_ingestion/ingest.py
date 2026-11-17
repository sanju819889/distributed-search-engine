#!/usr/bin/env python3
"""
Node 1: Ingestion & Data Quality Pipeline
Author: Sanjeev (Node 1 Lead)

Requirements Satisfied:
1. Parse and validate input records from JSONL.
2. Reject malformed records or missing required fields.
3. Deduplicate by document ID (highest version wins).
4. Remove links to documents that do not exist (dead-link cleaning).
5. Merge accepted changes into Apache Iceberg table (idempotent: re-running does not corrupt or duplicate data).
"""

import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, udf
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, ArrayType
)

def create_spark_session(master_url=None, warehouse_uri="hdfs://node2:9000/iceberg"):
    builder = (
        SparkSession.builder
        .appName("Node1-Ingestion-Pipeline")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse_uri)
    )
    if master_url:
        builder = builder.master(master_url)
    return builder.getOrCreate()

def run_ingestion(input_path, phase_name="phase0", master_url=None, warehouse_uri="hdfs://node2:9000/iceberg"):
    print(f"==================================================")
    print(f"[*] Starting Ingestion Pipeline: Phase '{phase_name}'")
    print(f"[*] Input file: {input_path}")
    print(f"[*] Iceberg warehouse: {warehouse_uri}")
    print(f"==================================================")

    spark = create_spark_session(master_url, warehouse_uri)
    spark.sparkContext.setLogLevel("WARN")

    # 1. Define Strict Expected Schema
    expected_schema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("url", StringType(), True),
        StructField("text", StringType(), True),
        StructField("links", ArrayType(StringType()), True),
        StructField("version", LongType(), True)
    ])

    # 2. Read and Validate
    raw_df = spark.read.schema(expected_schema).json(input_path)
    total_raw = raw_df.count()

    # Reject records missing id, title, text, or version
    valid_df = raw_df.filter(
        col("id").isNotNull() &
        (col("id") != "") &
        col("title").isNotNull() &
        col("text").isNotNull() &
        col("version").isNotNull()
    )
    total_valid = valid_df.count()
    rejected_count = total_raw - total_valid
    print(f"[*] Total raw records: {total_raw}")
    print(f"[*] Valid records: {total_valid} | Rejected (dirty/missing fields): {rejected_count}")

    # 3. Deduplication: By document ID, highest version wins
    window_spec = Window.partitionBy("id").orderBy(col("version").desc())
    deduped_df = (
        valid_df
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") == 1)
        .drop("rank")
    )
    total_deduped = deduped_df.count()
    print(f"[*] Deduplicated records (highest version kept): {total_deduped}")

    # 4. Clean Dangling Links
    # Only keep links that point to documents present in this corpus
    all_doc_ids = set([row["id"] for row in deduped_df.select("id").distinct().collect()])

    # UDF to filter out dead links
    def filter_valid_links(links_arr):
        if not links_arr:
            return []
        return [l for l in links_arr if l in all_doc_ids]

    clean_links_udf = udf(filter_valid_links, ArrayType(StringType()))
    cleaned_df = deduped_df.withColumn("links", clean_links_udf(col("links")))

    # 5. Idempotent Apache Iceberg Merge
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.default")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.default.documents (
            id STRING,
            title STRING,
            url STRING,
            text STRING,
            links ARRAY<STRING>,
            version BIGINT
        )
        USING iceberg
    """)

    cleaned_df.createOrReplaceTempView("batch_updates")

    # MERGE INTO guarantees that if Phase 0 or Phase 1 is submitted twice,
    # it updates to highest version and does NOT create duplicate records!
    merge_query = """
        MERGE INTO lakehouse.default.documents target
        USING batch_updates source
        ON target.id = source.id
        WHEN MATCHED AND source.version >= target.version THEN
            UPDATE SET 
                target.title = source.title,
                target.url = source.url,
                target.text = source.text,
                target.links = source.links,
                target.version = source.version
        WHEN NOT MATCHED THEN
            INSERT (id, title, url, text, links, version)
            VALUES (source.id, source.title, source.url, source.text, source.links, source.version)
    """

    print("[*] Executing idempotent Iceberg MERGE INTO table 'lakehouse.default.documents'...")
    spark.sql(merge_query)

    final_count = spark.sql("SELECT COUNT(*) as count FROM lakehouse.default.documents").collect()[0]["count"]
    print(f"[✓] Ingestion successful! Current table record count: {final_count}")
    print(f"==================================================")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Node 1 Ingestion Pipeline")
    parser.add_argument("input_path", help="Path to input jsonl file (e.g. data/pages.jsonl)")
    parser.add_argument("--phase", default="phase0", help="Phase name (phase0 or phase1)")
    parser.add_argument("--master", default=None, help="Spark master URL (e.g. spark://node3:7077)")
    parser.add_argument("--warehouse", default="hdfs://node2:9000/iceberg", help="Iceberg warehouse URI")
    args = parser.parse_args()

    run_ingestion(args.input_path, args.phase, args.master, args.warehouse)
