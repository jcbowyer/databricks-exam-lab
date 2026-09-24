# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · All join types
# MAGIC **Related exam objectives:** Spark Developer Associate, Developing Apache Spark DataFrame/DataSet API Applications (30%)
# MAGIC
# MAGIC **Learning objective:** use inner, left, right, full outer, cross, `left_semi` and `left_anti` joins. Avoid ambiguous columns and know which rows each join keeps.
# MAGIC
# MAGIC **Say it out loud:** "Semi and anti joins are filters, not joins. They return only left-side columns and never duplicate left rows.
# MAGIC `left_semi` means 'has a match' (EXISTS) and `left_anti` means 'has no match' (NOT EXISTS). In TPC-H about a third of customers never order,
# MAGIC so `customer left_anti orders` is the classic 'customers with no orders' query."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F

customer = spark.table("customer").alias("c")
orders = spark.table("orders").alias("o")
nation = spark.table("nation").alias("n")
region = spark.table("region").alias("r")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked examples

# COMMAND ----------

# INNER: only matching rows. Aliases + qualified names avoid "ambiguous column" errors.
inner = nation.join(region, F.col("n.n_regionkey") == F.col("r.r_regionkey"), "inner") \
              .select("n.n_name", "r.r_name")
print("inner:", inner.count())                                   # 25

# LEFT / RIGHT / FULL: keep unmatched rows from one or both sides, with NULLs on the other.
europe = region.filter("r_name = 'EUROPE'")
left = nation.join(europe, F.col("n.n_regionkey") == F.col("r.r_regionkey"), "left")
print("left keeps all 25 nations:", left.count(), "| non-EUROPE rows with NULL r_name:", left.filter("r_name IS NULL").count())
right = nation.filter("n_nationkey < 3").join(region, F.col("n.n_regionkey") == F.col("r.r_regionkey"), "right")
print("right keeps all 5 regions:", right.select("r.r_name").distinct().count())
full = nation.filter("n_nationkey < 3").join(europe, F.col("n.n_regionkey") == F.col("r.r_regionkey"), "full")
display(full.select("n.n_name", "r.r_name"))

# COMMAND ----------

# SEMI and ANTI: filter customers by whether they have orders. Only customer columns come back.
with_orders = customer.join(orders, F.col("c.c_custkey") == F.col("o.o_custkey"), "left_semi")
without_orders = customer.join(orders, F.col("c.c_custkey") == F.col("o.o_custkey"), "left_anti")
print("customers:", customer.count(), "| with orders:", with_orders.count(), "| without orders:", without_orders.count())
print(without_orders.columns)                                    # customer columns only, no duplicates

# COMMAND ----------

# CROSS: every combination. Use deliberately and only on small inputs.
segments = customer.select("c_mktsegment").distinct()
print("regions x segments:", region.crossJoin(segments).count())  # 5 x 5 = 25

# Joining on a column name (string or list) merges the key into ONE output column.
same_name = spark.table("customer").withColumnRenamed("c_nationkey", "nationkey") \
    .join(spark.table("nation").withColumnRenamed("n_nationkey", "nationkey"), "nationkey")
print("key appears once:", same_name.columns.count("nationkey") == 1)

# COMMAND ----------

without_orders.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** `SortMergeJoin ... LeftAnti` or `BroadcastHashJoin ... LeftAnti`, depending on size (AQE may switch at runtime),
# MAGIC and the `Project` above it keeps only `customer` columns. A `left_semi` shows `LeftSemi` in the same position.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# supplier = spark.table("supplier").alias("s")
# partsupp = spark.table("partsupp").alias("ps")
# # TODO: suppliers that supply at least one part (left_semi on s_suppkey = ps_suppkey)
# # TODO: nations with no suppliers (left_anti nation -> supplier on n_nationkey = s_nationkey)
# # TODO: nation LEFT join supplier, count suppliers per nation (count(s_suppkey) so unmatched counts 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **Semi join vs inner join + distinct?** Semi never duplicates left rows and needs no distinct; it can stop at the first match.
# MAGIC - **`NOT IN` vs `left_anti` with NULLs?** `NOT IN` returns nothing if the subquery has a NULL; anti join (NOT EXISTS) handles NULLs as you expect.
# MAGIC - **Ambiguous column error?** Alias both sides (`.alias("c")`) and select `c.col`, or join on a column-name string.
# MAGIC - **Full outer join strategy?** It cannot broadcast either side, so it is usually a sort-merge join.
