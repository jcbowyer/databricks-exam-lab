# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Declarative Pipeline with expectations
# MAGIC **Exam:** Data Engineer Professional, Data Transformation, Cleansing, and Quality (10%)
# MAGIC
# MAGIC **Learning objective:** build bronze → silver → gold on `customer` + `nation` with the three expectation actions:
# MAGIC **warn** (`@dp.expect`), **drop** (`@dp.expect_or_drop`) and **fail** (`@dp.expect_or_fail`).
# MAGIC
# MAGIC **Say it out loud:** "A declarative pipeline declares *what* each table is; the engine works out the dependency graph, checkpoints,
# MAGIC retries and incremental refresh. Expectations are SQL predicates on each dataset: warn keeps the bad rows but records metrics,
# MAGIC drop filters them out, and fail stops the update so nothing bad gets published."
# MAGIC
# MAGIC ⚠️ **This file is pipeline source, not a job notebook.** `resources/pipelines.yml` lists it under `libraries`, and the
# MAGIC `data_engineer_professional` job runs it through a `pipeline_task`. Running it interactively on ordinary compute fails.
# MAGIC Pipelines get parameters from `configuration` (via `spark.conf.get`), not from widgets, and the default catalog and schema come from the pipeline settings.
# MAGIC Input files are written by `02_data_ingestion_and_acquisition/01_uc_volume_setup`.

# COMMAND ----------

from pyspark import pipelines as dp  # legacy equivalent: import dlt
from pyspark.sql import functions as F

landing_path = spark.conf.get("landing_path")  # set in resources/pipelines.yml -> configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: raw files → streaming tables (Auto Loader)
# MAGIC Pipelines manage the Auto Loader `schemaLocation` and checkpoint for you, so neither is set here.

# COMMAND ----------

@dp.table(comment="Raw customer CSV files, loaded incrementally with Auto Loader")
def bronze_customer():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{landing_path}/customer_csv")
        .withColumn("_source_file", F.col("_metadata.file_path"))  # input_file_name() is not supported in UC
    )


@dp.table(comment="Raw nation JSON files")
def bronze_nation():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{landing_path}/nation_json")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: typed and validated, with all three expectation actions

# COMMAND ----------

@dp.table(comment="Cleaned customers")
@dp.expect("non_negative_balance", "c_acctbal >= 0")                         # WARN: keep the row, count the violation
@dp.expect_or_drop("valid_key", "c_custkey IS NOT NULL")                     # DROP: remove the row
@dp.expect_or_drop("known_nation", "c_nationkey BETWEEN 0 AND 24")           # DROP
@dp.expect_or_fail("has_name", "c_name IS NOT NULL")                         # FAIL: stop the update
def silver_customer():
    return spark.readStream.table("bronze_customer").select(
        F.col("c_custkey").cast("bigint"),
        F.trim("c_name").alias("c_name"),
        F.col("c_nationkey").cast("int"),
        F.col("c_acctbal").cast("decimal(18,2)"),
        F.upper("c_mktsegment").alias("c_mktsegment"),
    )


@dp.materialized_view(comment="Cleaned nations (small, full recompute is fine)")
@dp.expect_or_drop("valid_region", "n_regionkey BETWEEN 0 AND 4")
def silver_nation():
    return spark.read.table("bronze_nation").select(
        F.col("n_nationkey").cast("int"), F.col("n_name"), F.col("n_regionkey").cast("int")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: business aggregate as a materialized view
# MAGIC Aggregations over a streaming table belong in a **materialized view** (batch read). On serverless, materialized views can refresh incrementally.

# COMMAND ----------

@dp.materialized_view(comment="Customer count and balance by nation")
def gold_customers_by_nation():
    customers = spark.read.table("silver_customer")
    nations = spark.read.table("silver_nation")
    return (
        customers.join(nations, customers.c_nationkey == nations.n_nationkey)
        .groupBy("n_name")
        .agg(F.count("*").alias("customer_count"), F.round(F.avg("c_acctbal"), 2).alias("avg_acctbal"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill (retype from memory; pipeline source must stay valid, so it is commented out)

# COMMAND ----------

# @dp.table()
# # TODO: warn when c_acctbal < 0, drop rows with a NULL c_custkey, fail if c_name is NULL
# def silver_customer_drill():
#     return spark.readStream.table("bronze_customer")  # TODO: select and cast the columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **Where do expectation metrics go?** The pipeline event log (`event_log()` table-valued function, `flow_progress` events → `data_quality`).
# MAGIC - **Streaming table vs materialized view?** A streaming table is append-only incremental; a materialized view is kept correct over the whole source (use it for aggregates).
# MAGIC - **Why not `expect_or_fail` everywhere?** One bad row stops the whole update; use it only for invariants, and use quarantine or drop for dirty data.
# MAGIC - **SQL syntax?** `CONSTRAINT valid_key EXPECT (c_custkey IS NOT NULL) ON VIOLATION DROP ROW` (or `FAIL UPDATE`; omit the clause to warn).
# MAGIC - **`dlt` vs `dp`?** `import dlt` is the legacy module name; `from pyspark import pipelines as dp` is the current one. The decorators match one-to-one.
