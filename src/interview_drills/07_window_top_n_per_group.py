# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Window: top-N per group
# MAGIC **Related exam objectives:** Spark Developer Associate, Developing Apache Spark DataFrame/DataSet API Applications (30%)
# MAGIC
# MAGIC **Learning objective:** rank customers within each nation by total spend and keep the top 3, and choose the right ranking function for ties.
# MAGIC
# MAGIC **Say it out loud:** "Top-N per group is aggregate first, then rank. I sum spend per customer, join to get the nation,
# MAGIC rank within each nation ordered by spend descending, and filter rank ≤ N. Which ranking function I pick decides
# MAGIC whether ties can return more than N rows."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import Window
from pyspark.sql import functions as F

TOP_N = 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: top 3 customers per nation by total spend

# COMMAND ----------

spend = (
    spark.table("orders")
    .groupBy("o_custkey")
    .agg(F.sum("o_totalprice").alias("total_spend"), F.count("*").alias("order_count"))
)

customer_spend = (
    spend.join(spark.table("customer"), spend.o_custkey == F.col("c_custkey"))
    .join(spark.table("nation"), F.col("c_nationkey") == F.col("n_nationkey"))
    .select("n_name", "c_custkey", "c_name", "total_spend", "order_count")
)

by_nation = Window.partitionBy("n_name").orderBy(F.desc("total_spend"))

top_customers = (
    customer_spend
    .withColumn("rank", F.dense_rank().over(by_nation))
    .filter(F.col("rank") <= TOP_N)
    .orderBy("n_name", "rank")
)
display(top_customers)

# COMMAND ----------

# Same query in SQL with QUALIFY.
customer_spend.createOrReplaceTempView("v_customer_spend")
display(spark.sql(f"""
    SELECT n_name, c_name, total_spend,
           dense_rank() OVER (PARTITION BY n_name ORDER BY total_spend DESC) AS rank
    FROM v_customer_spend
    QUALIFY rank <= {TOP_N}
    ORDER BY n_name, rank
"""))

# COMMAND ----------

# Compare the ranking functions on the same window: they differ only when there are ties.
display(
    customer_spend.withColumn("spend_rounded", F.round("total_spend", -5))   # round to force ties
    .withColumns({
        "row_number": F.row_number().over(Window.partitionBy("n_name").orderBy(F.desc("spend_rounded"))),
        "rank": F.rank().over(Window.partitionBy("n_name").orderBy(F.desc("spend_rounded"))),
        "dense_rank": F.dense_rank().over(Window.partitionBy("n_name").orderBy(F.desc("spend_rounded"))),
    })
    .filter("n_name = 'FRANCE' AND row_number <= 8")
    .select("c_name", "spend_rounded", "row_number", "rank", "dense_rank")
)

# COMMAND ----------

top_customers.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** one `Exchange` for the aggregation (`o_custkey`), exchanges or broadcasts for the joins, then
# MAGIC `Exchange hashpartitioning(n_name)` → `Sort` → `Window [dense_rank()]` → `Filter`. With only 25 nations there are at most 25 useful partitions after that shuffle.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# # Top 5 suppliers per region by total available quantity (partsupp -> supplier -> nation -> region).
# supplier_qty = (spark.table("partsupp")
#     # TODO: groupBy ps_suppkey, sum ps_availqty as total_qty
# )
# # TODO: join supplier, nation, region to get r_name
# # TODO: window partitioned by r_name ordered by total_qty desc; keep row_number <= 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **Exactly N rows even with ties?** Use `row_number` with a deterministic tie-breaker.
# MAGIC - **Include everyone tied at position N?** Use `rank` or `dense_rank`.
# MAGIC - **Global top-N (not per group)?** `orderBy(desc).limit(N)`, which Spark plans as `TakeOrderedAndProject` with no full sort.
# MAGIC - **Why aggregate before ranking?** Ranking works on rows; spend lives across many orders, so aggregate to one row per customer first.
