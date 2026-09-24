# Databricks notebook source
# MAGIC %md
# MAGIC # 13 · repartition vs coalesce, and reading shuffles in explain()
# MAGIC **Related exam objectives:** Spark Developer Associate, Apache Spark Architecture and Components (20%); Troubleshooting and Tuning Apache Spark DataFrame API Applications (10%)
# MAGIC
# MAGIC **Learning objective:** change the partition count, know which change causes a shuffle, and find the shuffles (`Exchange`) in a physical plan.
# MAGIC
# MAGIC **Say it out loud:** "`repartition(n)` always shuffles and gives evenly sized partitions. It can increase or decrease the count, or hash by a column.
# MAGIC `coalesce(n)` only merges existing partitions without a shuffle, so it is cheap but can leave uneven partitions and reduce parallelism upstream.
# MAGIC In `explain()` every shuffle shows up as an `Exchange` node."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F

customer = spark.table("customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Counting partitions without RDDs
# MAGIC `df.rdd.getNumPartitions()` is **not available** on serverless (Spark Connect has no RDD API). Instead, tag each row with
# MAGIC `spark_partition_id()` and aggregate, which also shows how evenly the rows are spread.

# COMMAND ----------

def partition_sizes(df):
    """Rows per partition as a small DataFrame (works on Spark Connect)."""
    return df.groupBy(F.spark_partition_id().alias("partition")).count().orderBy("partition")

print("as read:", partition_sizes(customer).count(), "partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example

# COMMAND ----------

round_robin = customer.repartition(8)                       # shuffle, round robin, even sizes
by_key = customer.repartition(8, "c_nationkey")             # shuffle, hash by key (same key -> same partition)
fewer = round_robin.coalesce(2)                             # no shuffle: merges neighbouring partitions
ranged = customer.repartitionByRange(4, "c_acctbal")        # shuffle, sorted ranges (sampling picks the boundaries)

for name, df in [("repartition(8)", round_robin), ("repartition(8, key)", by_key), ("coalesce(2)", fewer), ("byRange(4)", ranged)]:
    sizes = [r["count"] for r in partition_sizes(df).collect()]
    print(f"{name:22s} partitions={len(sizes):2d} min={min(sizes)} max={max(sizes)}")

# COMMAND ----------

print("---- repartition(8) ----")
round_robin.explain()
print("---- repartition(8, c_nationkey) ----")
by_key.explain()
print("---- repartition(8).coalesce(2) ----")
fewer.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:**
# MAGIC - `Exchange RoundRobinPartitioning(8)` for `repartition(8)`.
# MAGIC - `Exchange hashpartitioning(c_nationkey, 8)` for `repartition(8, col)`. With 25 nations hashed into 8 partitions, sizes are uneven (skew).
# MAGIC - `Coalesce 2` with **no** new `Exchange` for `coalesce(2)`.
# MAGIC - `groupBy`, joins, `distinct` and windows also add `Exchange` nodes. That is where the stage boundaries are.

# COMMAND ----------

# Shuffle partitions: the number of partitions a wide transformation produces (serverless allows setting this one).
try:
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    print("shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
except Exception as e:
    print("Config not settable here:", type(e).__name__)
customer.groupBy("c_nationkey").count().explain()           # AQE may coalesce the 16 at runtime (see query profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# # TODO: repartition customer into 4 partitions hashed by c_mktsegment
# # TODO: show rows per partition with spark_partition_id()
# # TODO: coalesce the result to 1 partition and confirm with explain() that no new Exchange appears

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **Which one before writing fewer files?** `coalesce` if the data is already balanced, `repartition` if it is skewed (pay one shuffle for even files).
# MAGIC - **Can `coalesce` increase partitions?** No, it can only reduce them.
# MAGIC - **What decides partitions after a shuffle?** `spark.sql.shuffle.partitions`, then AQE coalesces small ones at runtime.
# MAGIC - **Why is `rdd.getNumPartitions()` missing on serverless?** Spark Connect exposes only the DataFrame/SQL API, with no RDDs or SparkContext.
