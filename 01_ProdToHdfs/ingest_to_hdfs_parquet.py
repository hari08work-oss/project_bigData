#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import sys
from pyspark.sql import SparkSession, functions as F, types as T

APP_NAME = "IngestToHDFS_Parquet"
HDFS = "hdfs://namenode:8020"

SRC_BASE = HDFS + "/landing"    # đọc CSV đã mv sang /landing/*
OUT_BASE = HDFS + "/source"     # ghi Parquet (partition y/m/d)

TABLES = {
    "leads": {
        "csv": SRC_BASE + "/leads/*.csv",
        "schema": T.StructType([
            T.StructField("lead_id",    T.StringType()),
            T.StructField("created_at", T.TimestampType()),
            T.StructField("channel",    T.StringType()),
            T.StructField("source",     T.StringType()),
            T.StructField("campaign",   T.StringType()),
        ]),
        "ts": "created_at",
        "out": OUT_BASE + "/leads"
    },
    "messages": {
        "csv": SRC_BASE + "/messages/*.csv",
        "schema": T.StructType([
            T.StructField("msg_id",    T.StringType()),
            T.StructField("lead_id",   T.StringType()),
            T.StructField("channel",   T.StringType()),
            T.StructField("msg_ts",    T.TimestampType()),
            T.StructField("from_side", T.StringType()),
        ]),
        "ts": "msg_ts",
        "out": OUT_BASE + "/messages"
    },
    "appointments": {
        "csv": SRC_BASE + "/appointments/*.csv",
        "schema": T.StructType([
            T.StructField("booking_id", T.StringType()),
            T.StructField("lead_id",    T.StringType()),
            T.StructField("booked_ts",  T.TimestampType()),
            T.StructField("status",     T.StringType()),
            T.StructField("service",    T.StringType()),
            T.StructField("revenue",    T.DoubleType()),
        ]),
        "ts": "booked_ts",
        "out": OUT_BASE + "/appointments"
    },
    "ads_spend": {
        "csv": SRC_BASE + "/ads_spend/*.csv",
        "schema": T.StructType([
            T.StructField("campaign", T.StringType()),
            T.StructField("dt",       T.DateType()),
            T.StructField("spend",    T.DoubleType()),
        ]),
        "ts": "dt",
        "out": OUT_BASE + "/ads_spend"
    },
}

def build_spark():
    return (SparkSession.builder
            .appName(APP_NAME)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate())

def read_csv(spark, path, schema):
    return (spark.read.format("csv")
            .option("header", "true")
            .option("multiLine", "false")
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
            .schema(schema)
            .load(path))

def add_partitions(df, ts_col):
    df = df.withColumn("event_date", F.to_date(F.col(ts_col)))
    return (df
            .withColumn("y", F.year("event_date"))
            .withColumn("m", F.month("event_date"))
            .withColumn("d", F.dayofmonth("event_date")))

def get_watermark(spark, out_path, ts_col):
    try:
        mx = spark.read.parquet(out_path).agg(F.max(F.col(ts_col))).first()[0]
        return mx
    except Exception:
        return None

def write_parquet(df, out_path, mode):
    (df.write.mode(mode)
       .partitionBy("y","m","d")
       .parquet(out_path))

def process_table(spark, name, mode):
    cfg = TABLES[name]
    df = read_csv(spark, cfg["csv"], cfg["schema"])
    df = add_partitions(df, cfg["ts"])

    if mode == "FullLoad":
        write_parquet(df, cfg["out"], "overwrite")
        return df.count()

    if mode == "IncrementalLoad":
        wm = get_watermark(spark, cfg["out"], cfg["ts"])
        out_df = df.filter(F.col(cfg["ts"]) > F.lit(wm)) if wm is not None else df
        n = out_df.count()
        if n > 0:
            write_parquet(out_df, cfg["out"], "append")
        return n

    raise ValueError("Mode must be FullLoad or IncrementalLoad")

def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("FullLoad", "IncrementalLoad"):
        print("Usage: spark-submit ingest_to_hdfs_parquet.py [FullLoad|IncrementalLoad]")
        sys.exit(1)

    spark = build_spark()
    mode = sys.argv[1]
    total = 0
    for t in ["leads","messages","appointments","ads_spend"]:
        cnt = process_table(spark, t, mode)
        print("[" + t + "] written rows = " + str(cnt))
        total += cnt
    print("Done. Total rows written: " + str(total))
    spark.stop()

if __name__ == "__main__":
    main()
