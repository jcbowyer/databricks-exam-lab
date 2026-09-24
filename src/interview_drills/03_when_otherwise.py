# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Conditional columns with when / otherwise
# MAGIC **Related exam objectives:** Spark Developer Associate, Developing Apache Spark DataFrame/DataSet API Applications (30%); Using Spark SQL (20%)
# MAGIC
# MAGIC **Learning objective:** build multi-branch conditional columns with `F.when().when().otherwise()` and the SQL `CASE WHEN` equivalent,
# MAGIC and know what happens when no branch matches.
# MAGIC
# MAGIC **Say it out loud:** "`when/otherwise` is the DataFrame form of SQL `CASE WHEN`. Branches are checked top-down and the first match wins,
# MAGIC and if nothing matches and there is no `otherwise`, the result is NULL. It runs inside the JVM as a native expression,
# MAGIC so it is always preferable to a Python UDF for bucketing logic."

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
# MAGIC ## Worked example: balance bands and segment groups

# COMMAND ----------

banded = customer.select(
    "c_custkey",
    "c_acctbal",
    "c_mktsegment",
    F.when(F.col("c_acctbal") < 0, "negative")               # first match wins, so order matters
     .when(F.col("c_acctbal") < 5000, "standard")
     .otherwise("premium")
     .alias("balance_band"),
    F.when(F.col("c_mktsegment").isin("BUILDING", "MACHINERY"), "industrial")
     .when(F.col("c_mktsegment") == "AUTOMOBILE", "auto")
     .alias("segment_group"),                                # no otherwise -> NULL for other segments
)
display(banded.groupBy("balance_band", "segment_group").count().orderBy("balance_band", "segment_group"))

# COMMAND ----------

# The SQL equivalent, run through a temp view.
customer.createOrReplaceTempView("v_customer")
display(spark.sql("""
    SELECT CASE WHEN c_acctbal < 0    THEN 'negative'
                WHEN c_acctbal < 5000 THEN 'standard'
                ELSE 'premium' END AS balance_band,
           count(*) AS customers
    FROM v_customer
    GROUP BY ALL
    ORDER BY balance_band
"""))

# COMMAND ----------

# Conditional aggregation: count per band in ONE pass (a pivot without pivot()).
display(
    customer.groupBy("c_mktsegment").agg(
        F.count(F.when(F.col("c_acctbal") < 0, True)).alias("negative"),
        F.count(F.when(F.col("c_acctbal") >= 5000, True)).alias("premium"),
        F.sum(F.when(F.col("c_acctbal") > 0, F.col("c_acctbal")).otherwise(0)).alias("positive_balance_total"),
    ).orderBy("c_mktsegment")
)

# COMMAND ----------

banded.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** the `Project` node contains `CASE WHEN (c_acctbal < 0) THEN negative ...`. The DataFrame `when` compiles to exactly the same
# MAGIC `CaseWhen` expression as SQL. There is no `BatchEvalPython` / `ArrowEvalPython` node, which would indicate a Python UDF.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# drill = customer.withColumn(
#     "size_label",
#     # TODO: 'small' when c_acctbal < 1000, 'medium' when < 7000, otherwise 'large'
# )
# # TODO: write the same logic with CASE WHEN in spark.sql over v_customer
# display(drill.groupBy("size_label").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **No `otherwise` and no match?** The result is NULL.
# MAGIC - **Why not a Python UDF for bucketing?** A UDF serialises rows to Python, blocks Catalyst optimisations and is much slower.
# MAGIC - **How do you count rows matching a condition?** `count(when(cond, True))` or `sum(cast(cond as int))`, because `count` ignores NULLs.
# MAGIC - **Branch order?** Top-down, first true wins, so put the narrowest conditions first.
