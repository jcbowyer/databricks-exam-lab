# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Window: latest record per key
# MAGIC **Related exam objectives:** Spark Developer Associate, Developing Apache Spark DataFrame/DataSet API Applications (30%); Data Engineer Professional, Developing Code for Data Processing using Python and SQL (22%)
# MAGIC
# MAGIC **Learning objective:** keep exactly one row per key (the most recent) with `row_number()` over a window. Know the SQL `QUALIFY` form
# MAGIC and the `max_by` shortcut.
# MAGIC
# MAGIC **Say it out loud:** "To get the latest record per key I partition by the key, order by the timestamp descending with a tie-breaker,
# MAGIC number the rows, and keep row 1. I use `row_number` and not `rank`, because rank can return two rows when timestamps tie.
# MAGIC This is the standard dedup step before a MERGE."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import Window
from pyspark.sql import functions as F

orders = spark.table("orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: each customer's most recent order

# COMMAND ----------

latest_w = Window.partitionBy("o_custkey").orderBy(F.col("o_orderdate").desc(), F.col("o_orderkey").desc())  # tie-breaker!

latest_order = (
    orders
    .withColumn("rn", F.row_number().over(latest_w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select("o_custkey", "o_orderkey", "o_orderdate", "o_orderstatus", "o_totalprice")
)
display(latest_order.orderBy("o_custkey").limit(10))

# Exactly one row per key:
assert latest_order.count() == orders.select("o_custkey").distinct().count()

# COMMAND ----------

# SQL: QUALIFY filters on a window function without a subquery.
orders.createOrReplaceTempView("v_orders")
display(spark.sql("""
    SELECT o_custkey, o_orderkey, o_orderdate, o_totalprice
    FROM v_orders
    QUALIFY row_number() OVER (PARTITION BY o_custkey ORDER BY o_orderdate DESC, o_orderkey DESC) = 1
    ORDER BY o_custkey
    LIMIT 10
"""))

# COMMAND ----------

# Shortcut when you only need a few columns: max_by(value, ordering) inside a groupBy (no window needed).
# Caveat: with several columns, each max_by is evaluated separately, and ties can come from different rows.
display(
    orders.groupBy("o_custkey").agg(
        F.max("o_orderdate").alias("last_order_date"),
        F.max_by("o_orderkey", "o_orderdate").alias("last_orderkey"),
    ).orderBy("o_custkey").limit(10)
)

# COMMAND ----------

latest_order.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** `Exchange hashpartitioning(o_custkey)` → `Sort [o_custkey, o_orderdate DESC, o_orderkey DESC]` → `Window [row_number() ...]`
# MAGIC → `Filter (rn = 1)`. Newer runtimes may show a `WindowGroupLimit` node that stops each partition after row 1, which is the rank-limit optimisation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# # Latest supplier price per part: partsupp has one row per (part, supplier), so take the cheapest supplier per part instead.
# partsupp = spark.table("partsupp")
# w = (Window
#      # TODO: partition by ps_partkey, order by ps_supplycost ascending, then ps_suppkey
#     )
# cheapest = (partsupp
#     # TODO: add row_number over w, keep rn == 1, drop rn
# )
# display(cheapest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **`row_number` vs `rank` vs `dense_rank`?** On ties, row_number gives 1,2 (unique), rank gives 1,1,3, and dense_rank gives 1,1,2.
# MAGIC - **Why a tie-breaker?** Without a unique ordering the result is non-deterministic between runs.
# MAGIC - **Why not `dropDuplicates(["key"])`?** It keeps an *arbitrary* row, with no control over which one is latest.
# MAGIC - **Cost?** One shuffle on the partition key plus a sort. Skewed keys make one task slow.
