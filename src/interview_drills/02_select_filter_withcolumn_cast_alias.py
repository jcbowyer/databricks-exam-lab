# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · select, filter, withColumn, cast, alias
# MAGIC **Related exam objectives:** Spark Developer Associate, Developing Apache Spark DataFrame/DataSet API Applications (30%)
# MAGIC
# MAGIC **Learning objective:** project, rename, derive and cast columns, and filter rows, in the DataFrame API and the SQL-expression style.
# MAGIC
# MAGIC **Say it out loud:** "Every one of these calls is a lazy, narrow transformation. It just adds a node to the logical plan.
# MAGIC Catalyst then collapses the chain into one `Project` and one `Filter`, and pushes the filter down to the scan,
# MAGIC so the order I write them in rarely matters for performance."

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
# MAGIC ## Worked example

# COMMAND ----------

wealthy_building = (
    customer
    .filter(F.col("c_mktsegment") == "BUILDING")                   # Column expression
    .where("c_acctbal > 5000")                                      # SQL string; where == filter
    .withColumn("acctbal_k", (F.col("c_acctbal") / 1000).cast("decimal(10,1)"))
    .withColumns({                                                  # several derived columns at once
        "name_upper": F.upper("c_name"),
        "country_code": F.substring("c_phone", 1, 2).cast("int"),
    })
    .select(
        F.col("c_custkey").alias("customer_id"),
        "name_upper",
        F.col("c_nationkey").alias("nation_id"),
        "acctbal_k",
        "country_code",
    )
)
display(wealthy_building.limit(10))

# COMMAND ----------

# The same thing in expression style: selectExpr accepts SQL snippets.
display(
    customer.selectExpr(
        "c_custkey AS customer_id",
        "upper(c_name) AS name_upper",
        "CAST(c_acctbal / 1000 AS DECIMAL(10,1)) AS acctbal_k",
    ).where("c_mktsegment = 'BUILDING' AND c_acctbal > 5000").limit(5)
)

# COMMAND ----------

# Combining conditions: use & | ~ with parentheses (Python's and/or do not work on Columns).
segment_filter = (F.col("c_mktsegment").isin("BUILDING", "MACHINERY")) & ~(F.col("c_acctbal") < 0)
print(customer.filter(segment_filter).count())

# Renaming and dropping
renamed = customer.withColumnRenamed("c_custkey", "customer_id").drop("c_comment", "c_address")
print(renamed.columns)

# COMMAND ----------

wealthy_building.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** a single `Project` and a single `Filter` (Catalyst combined the chained calls), and
# MAGIC `PushedFilters: [IsNotNull(c_mktsegment), EqualTo(c_mktsegment,BUILDING), GreaterThan(c_acctbal,...)]` on the scan.
# MAGIC There is no `Exchange`, because every operation here is narrow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# drill = (customer
#     # TODO: keep only AUTOMOBILE customers with a negative balance
#     # TODO: add balance_abs = abs(c_acctbal) cast to decimal(18,2)
#     # TODO: select c_custkey aliased customer_id, c_name, balance_abs
# )
# display(drill)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **`withColumn` in a loop?** Each call adds a projection, and hundreds of them bloat the plan. Use `withColumns({...})` or one `select`.
# MAGIC - **`col("x")` vs `"x"` vs `df.x`?** They are equivalent in most APIs; `df.x` binds to a specific DataFrame, which helps disambiguate joins.
# MAGIC - **What does `cast` do on bad input?** With ANSI mode on (the serverless default) it errors; use `try_cast` to get NULL instead.
# MAGIC - **`filter` vs `where`?** They are aliases.
# MAGIC - **Is `select` a transformation?** Yes, lazy and narrow; nothing runs until an action.
