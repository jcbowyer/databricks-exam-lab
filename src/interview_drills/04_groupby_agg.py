# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · groupBy().agg() with multiple aggregates
# MAGIC **Related exam objectives:** Spark Developer Associate, Developing Apache Spark DataFrame/DataSet API Applications (30%)
# MAGIC
# MAGIC **Learning objective:** compute several aggregates in one pass, filter on aggregates (the HAVING equivalent), and read a two-phase aggregation plan.
# MAGIC
# MAGIC **Say it out loud:** "`groupBy` is a wide transformation. Spark first does a partial aggregate on each partition, then shuffles
# MAGIC by the grouping key and does a final merge. Putting every aggregate in one `agg()` call means one scan and one shuffle,
# MAGIC not one per metric."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F

customer = spark.table("customer")
nation = spark.table("nation")
partsupp = spark.table("partsupp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: customer statistics per nation

# COMMAND ----------

by_nation = (
    customer.join(nation, customer.c_nationkey == nation.n_nationkey)
    .groupBy("n_name")
    .agg(
        F.count("*").alias("customers"),
        F.round(F.avg("c_acctbal"), 2).alias("avg_balance"),
        F.min("c_acctbal").alias("min_balance"),
        F.max("c_acctbal").alias("max_balance"),
        F.sum("c_acctbal").alias("total_balance"),
        F.countDistinct("c_mktsegment").alias("segments"),
        F.collect_set("c_mktsegment").alias("segment_list"),
    )
    .filter(F.col("customers") > 1000)                   # HAVING: filter after agg
    .orderBy(F.desc("total_balance"))
)
display(by_nation)

# COMMAND ----------

# Several grouping keys + dictionary syntax (quick, but you cannot alias the result names).
display(partsupp.groupBy("ps_suppkey").agg({"ps_availqty": "sum", "ps_supplycost": "avg"}).limit(5))

# SQL with HAVING and GROUP BY ALL
nation.createOrReplaceTempView("v_nation")
customer.createOrReplaceTempView("v_customer")
display(spark.sql("""
    SELECT n.n_name, c.c_mktsegment, count(*) AS customers, round(avg(c.c_acctbal), 2) AS avg_balance
    FROM v_customer c JOIN v_nation n ON c.c_nationkey = n.n_nationkey
    GROUP BY ALL
    HAVING count(*) > 250
    ORDER BY customers DESC
    LIMIT 10
"""))

# COMMAND ----------

# Rollup adds subtotals (nation-level and grand total rows have NULL keys).
display(
    customer.rollup("c_nationkey", "c_mktsegment").agg(F.count("*").alias("customers"))
    .orderBy(F.col("c_nationkey").asc_nulls_last(), F.col("c_mktsegment").asc_nulls_last()).limit(12)
)

# COMMAND ----------

customer.groupBy("c_nationkey").agg(F.count("*"), F.avg("c_acctbal")).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** `HashAggregate(keys=[c_nationkey], functions=[partial_count(1), partial_avg(...)])` → `Exchange hashpartitioning(c_nationkey, ...)`
# MAGIC → `HashAggregate(... functions=[count(1), avg(...)])`. That is partial → shuffle → final. `countDistinct` adds an extra aggregation level.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# drill = (partsupp
#     # TODO: group by ps_suppkey
#     # TODO: agg: count parts, sum ps_availqty as total_qty, avg ps_supplycost rounded to 2 as avg_cost
#     # TODO: keep suppliers with total_qty > 400000, order by avg_cost desc
# )
# display(drill)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **`count("*")` vs `count("col")`?** `*` counts rows; `count(col)` skips NULLs.
# MAGIC - **Why is `countDistinct` expensive?** It needs an extra shuffle or aggregation level; `approx_count_distinct` (HyperLogLog) is much cheaper.
# MAGIC - **HAVING in the DataFrame API?** `.filter()` after `.agg()`.
# MAGIC - **`rollup` vs `cube`?** `rollup` gives hierarchical subtotals; `cube` gives every combination of the grouping columns.
