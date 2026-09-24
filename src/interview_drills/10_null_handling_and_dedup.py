# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Null handling and de-duplication
# MAGIC **Related exam objectives:** Spark Developer Associate, Developing Apache Spark DataFrame/DataSet API Applications (30%)
# MAGIC
# MAGIC **Learning objective:** fill, drop and coalesce NULLs, and choose between `distinct()` and `dropDuplicates(subset)`.
# MAGIC
# MAGIC **Say it out loud:** "`na.fill` and `na.drop` are bulk tools. `coalesce` picks the first non-NULL value per row, which is how I apply fallbacks.
# MAGIC `distinct` compares whole rows. `dropDuplicates` with a subset keeps one arbitrary row per key, so when *which* row survives matters,
# MAGIC I use a `row_number` window instead."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F

# TPC-H has no NULLs, so inject some: blank out ~10% of balances and ~5% of segments.
customer = spark.table("customer").select(
    "c_custkey", "c_name", "c_nationkey",
    F.when(F.col("c_custkey") % 10 == 0, None).otherwise(F.col("c_acctbal")).alias("c_acctbal"),
    F.when(F.col("c_custkey") % 20 == 5, None).otherwise(F.col("c_mktsegment")).alias("c_mktsegment"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: NULLs

# COMMAND ----------

# Count NULLs per column in one pass.
display(customer.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in customer.columns]))

filled = customer.na.fill({"c_acctbal": 0, "c_mktsegment": "UNKNOWN"})     # per-column defaults
dropped_any = customer.na.drop(how="any")                                    # drop if ANY column is NULL
dropped_subset = customer.na.drop(subset=["c_mktsegment"])                   # only look at these columns
kept_thresh = customer.na.drop(thresh=5)                                     # keep rows with >= 5 non-NULLs
print(customer.count(), dropped_any.count(), dropped_subset.count(), kept_thresh.count())

# coalesce: first non-NULL value, row by row (not the same as DataFrame.coalesce(n)!)
display(customer.select(
    "c_custkey",
    F.coalesce("c_mktsegment", F.lit("UNKNOWN")).alias("segment"),
    F.coalesce("c_acctbal", F.lit(0)).alias("balance"),
).filter("c_custkey % 10 = 0").limit(5))

# COMMAND ----------

# NULL comparisons: = returns NULL (treated as false in filters); use isNull or the null-safe <=> / eqNullSafe.
print("= NULL rows:", customer.filter(F.col("c_mktsegment") == None).count())               # always 0
print("isNull rows:", customer.filter(F.col("c_mktsegment").isNull()).count())
print("eqNullSafe rows:", customer.filter(F.col("c_mktsegment").eqNullSafe(None)).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: de-duplication

# COMMAND ----------

dupes = customer.unionByName(customer.filter("c_custkey % 3 = 0"))          # duplicate a third of the rows
print("rows:", dupes.count())
print("distinct():", dupes.distinct().count())                              # whole-row comparison
print("dropDuplicates(['c_nationkey']):", dupes.dropDuplicates(["c_nationkey"]).count())  # one row per nation (arbitrary!)

dupes.dropDuplicates(["c_custkey"]).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** `dropDuplicates(subset)` plans as `HashAggregate(keys=[c_custkey], functions=[first(...)])` around an `Exchange`.
# MAGIC That `first()` is why the surviving row is arbitrary. `distinct()` shows the same shape, with every column as a key.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# drill = (customer
#     # TODO: fill c_acctbal NULLs with 0 and c_mktsegment NULLs with 'UNKNOWN' using na.fill(dict)
#     # TODO: add segment_or_nation = coalesce(c_mktsegment, cast(c_nationkey as string))
# )
# # TODO: count rows where c_acctbal IS NULL using isNull (not == None)
# # TODO: remove duplicate customers from `dupes` keeping one row per c_custkey

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **`na.fill(0)` on string columns?** It only fills columns whose type matches the value; use a dict for mixed types.
# MAGIC - **`F.coalesce` vs `df.coalesce(n)`?** The first picks a non-NULL value per row; the second reduces the number of partitions.
# MAGIC - **Deterministic dedup?** `row_number()` over a key with an explicit ordering, keeping row 1.
# MAGIC - **Streaming dedup?** `dropDuplicatesWithinWatermark` (or `dropDuplicates` plus a watermark) to bound state.
