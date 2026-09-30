# Databricks notebook source
"""One availableNow serverless job run: provision, seed, generate, and process."""

import json
import os
import random
import re
import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema_name", "telematics_dev")
dbutils.widgets.text("volume_name", "landing_dev")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
for value in (catalog, schema_name, volume_name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Catalog, schema, and volume names must be simple SQL identifiers")

spark.conf.set("spark.sql.session.timeZone", "UTC")
namespace = f"{catalog}.{schema_name}"
volume_root = f"/Volumes/{catalog}/{schema_name}/{volume_name}"
bronze = f"{namespace}.bronze_pings"
silver = f"{namespace}.silver_pings"
gold = f"{namespace}.gold_geofence_pings"
details = f"{namespace}.truck_details"


def create_tables():
    # The schema and volume are declared as bundle resources. Tables are
    # versioned DDL in this bundle-owned job and never hand-created in the UI.
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {details} (
      truck_id STRING, make STRING, model STRING, capacity_lbs INT,
      home_depot STRING, region STRING, driver STRING
    ) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {bronze} (
      truck_id STRING, latitude DOUBLE, longitude DOUBLE, event_ts STRING,
      _rescued_data STRING, source_file STRING, ingested_at TIMESTAMP
    ) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {silver} (
      ping_id STRING, truck_id STRING, latitude DOUBLE, longitude DOUBLE,
      event_ts TIMESTAMP, source_file STRING, ingested_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {gold} (
      ping_id STRING, truck_id STRING, latitude DOUBLE, longitude DOUBLE,
      event_ts TIMESTAMP, make STRING, model STRING, capacity_lbs INT,
      home_depot STRING, region STRING, driver STRING,
      source_file STRING, ingested_at TIMESTAMP
    ) USING DELTA""")


def seed_trucks():
    makes = [("Freightliner", "Cascadia"), ("Volvo", "VNL"),
             ("Kenworth", "T680"), ("Peterbilt", "579")]
    depots = [("Chicago", "Midwest"), ("Dallas", "South"),
              ("Denver", "West"), ("Atlanta", "Southeast")]
    drivers = ["A. Rivera", "B. Chen", "C. Okafor", "D. Patel", "E. Nguyen",
               "F. Santos", "G. Kim", "H. Brooks", "I. Novak", "J. Alvarez"]
    rng = random.Random(42)
    rows = []
    for i in range(1, 21):
        make, model = rng.choice(makes)
        depot, region = rng.choice(depots)
        rows.append((f"TRK-{i:03d}", make, model,
                     rng.choice([20000, 26000, 34000, 40000]),
                     depot, region, rng.choice(drivers)))
    frame = spark.createDataFrame(
        rows, "truck_id STRING, make STRING, model STRING, capacity_lbs INT, "
              "home_depot STRING, region STRING, driver STRING"
    )
    (DeltaTable.forName(spark, details).alias("t")
     .merge(frame.alias("s"), "t.truck_id = s.truck_id")
     .whenNotMatchedInsertAll()
     .execute())


def generate_pings():
    # A new immutable file is emitted each run. Include an exact duplicate
    # and an invalid latitude so Silver behavior can be demonstrated.
    landing = f"{volume_root}/pings"
    os.makedirs(landing, exist_ok=True)
    rng = random.Random()
    for part in range(2):
        rows = []
        for _ in range(10):
            rows.append({
                "truck_id": f"TRK-{rng.randint(1, 20):03d}",
                "latitude": round(41.85 + rng.uniform(-0.2, 0.2), 6),
                "longitude": round(-87.65 + rng.uniform(-0.2, 0.2), 6),
                "event_ts": datetime.now(timezone.utc).isoformat()
            })
        rows.append(dict(rows[0]))
        rows.append({**rows[1], "latitude": 95.0})
        path = f"{landing}/pings_{uuid.uuid4().hex}_{part}.json"
        with open(path, "w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")
        print(f"Generated {len(rows)} records in {path}")


def load_bronze():
    # 'rescue' retains unexpected fields in _rescued_data. Promotion into
    # typed columns happens deliberately in a versioned table migration.
    incoming = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{volume_root}/_schemas/bronze")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints",
                "truck_id STRING, latitude DOUBLE, longitude DOUBLE, event_ts STRING")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{volume_root}/pings")
        .select("truck_id", "latitude", "longitude", "event_ts",
                "_rescued_data", F.col("_metadata.file_path").alias("source_file"))
        .withColumn("ingested_at", F.current_timestamp())
    )
    (incoming.writeStream.format("delta").outputMode("append")
     .option("checkpointLocation", f"{volume_root}/_checkpoints/bronze")
     .trigger(availableNow=True).toTable(bronze).awaitTermination())


def load_silver():
    clean = (
        spark.readStream.table(bronze)
        .select(
            F.trim("truck_id").alias("truck_id"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.expr("try_to_timestamp(event_ts)").alias("event_ts"),
            "source_file", "ingested_at"
        )
        .filter(
            (F.length("truck_id") > 0) &
            F.col("event_ts").isNotNull() &
            F.col("latitude").between(-90.0, 90.0) &
            F.col("longitude").between(-180.0, 180.0)
        )
        .withColumn(
            "ping_id",
            F.sha2(F.to_json(F.struct("truck_id", "latitude",
                                     "longitude", "event_ts")), 256)
        )
        .select("ping_id", "truck_id", "latitude", "longitude",
                "event_ts", "source_file", "ingested_at")
    )

    def merge_batch(batch, batch_id):
        unique = batch.dropDuplicates(["ping_id"])
        (DeltaTable.forName(spark, silver).alias("t")
         .merge(unique.alias("s"), "t.ping_id = s.ping_id")
         .whenNotMatchedInsertAll()
         .execute())

    (clean.writeStream.foreachBatch(merge_batch)
     .option("checkpointLocation", f"{volume_root}/_checkpoints/silver")
     .trigger(availableNow=True).start().awaitTermination())


def load_gold():
    # CDF safely streams INSERTs produced by Silver MERGE. Delta truck_details
    # is the static side; Databricks uses its latest valid snapshot per batch.
    new_silver = (
        spark.readStream.option("readChangeFeed", "true").table(silver)
        .filter(F.col("_change_type") == "insert")
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )
    enriched = (
        new_silver.join(F.broadcast(spark.table(details)), "truck_id", "left")
        .select("ping_id", "truck_id", "latitude", "longitude", "event_ts",
                "make", "model", "capacity_lbs", "home_depot", "region",
                "driver", "source_file", "ingested_at")
    )

    def merge_batch(batch, batch_id):
        (DeltaTable.forName(spark, gold).alias("t")
         .merge(batch.dropDuplicates(["ping_id"]).alias("s"),
                "t.ping_id = s.ping_id")
         .whenNotMatchedInsertAll()
         .execute())

    (enriched.writeStream.foreachBatch(merge_batch)
     .option("checkpointLocation", f"{volume_root}/_checkpoints/gold")
     .trigger(availableNow=True).start().awaitTermination())


def verify():
    bronze_count = spark.table(bronze).count()
    silver_count = spark.table(silver).count()
    gold_count = spark.table(gold).count()
    duplicate_count = spark.sql(
        f"SELECT COUNT(*) FROM (SELECT ping_id FROM {silver} "
        "GROUP BY ping_id HAVING COUNT(*) > 1)"
    ).first()[0]
    bad_coords = spark.table(silver).filter(
        ~F.col("latitude").between(-90.0, 90.0) |
        ~F.col("longitude").between(-180.0, 180.0)
    ).count()
    missing_detail = spark.table(gold).filter(F.col("driver").isNull()).count()
    print(f"Bronze={bronze_count}, Silver={silver_count}, Gold={gold_count}")
    print(f"Silver duplicates={duplicate_count}, bad coords={bad_coords}, "
          f"unenriched Gold rows={missing_detail}")
    assert bronze_count > silver_count > 0
    assert silver_count == gold_count
    assert duplicate_count == bad_coords == missing_detail == 0
    display(spark.sql(
        f"SELECT truck_id, event_ts, latitude, longitude, driver, "
        f"home_depot, region FROM {gold} ORDER BY event_ts DESC LIMIT 10"
    ))


create_tables()
seed_trucks()
generate_pings()
load_bronze()
load_silver()
load_gold()
verify()
