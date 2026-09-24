# Databricks notebook source
# MAGIC %md
# MAGIC # 14 · Broadcast join hint and AQE
# MAGIC **Related exam objectives:** Spark Developer Associate, Troubleshooting and Tuning Apache Spark DataFrame API Applications (10%); Data Engineer Professional, Cost & Performance Optimisation (13%)
# MAGIC
# MAGIC **Learning objective:** force a broadcast hash join with `broadcast()` or a SQL hint, recognise join strategies in the plan, and understand what
# MAGIC Adaptive Query Execution changes at runtime.
# MAGIC
# MAGIC **Say it out loud:** "When one side of a join is small, broadcasting it ships a full copy to every executor, so the big side never shuffles.
# MAGIC That is a `BroadcastHashJoin` rather than a `SortMergeJoin`. AQE re-plans between stages using real sizes: it can switch to a broadcast join,
# MAGIC coalesce small shuffle partitions, and split skewed partitions. The `broadcast()` function is a *join hint*.
# MAGIC It is not the RDD-era broadcast variable, which doesn't exist on serverless."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F

customer = spark.table("customer")
nation = spark.table("nation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AQE settings (only set what serverless allows)
# MAGIC Serverless enables AQE by default and may reject some configs. Each attempt is wrapped, so the notebook still runs and prints what happened.

# COMMAND ----------

def try_set(key, value):
    try:
        spark.conf.set(key, value)
        return f"set -> {spark.conf.get(key)}"
    except Exception as e:
        return f"not settable on this compute ({type(e).__name__})"

for key, value in {
    "spark.sql.adaptive.enabled": "true",                       # AQE master switch
    "spark.sql.adaptive.coalescePartitions.enabled": "true",    # merge tiny shuffle partitions
    "spark.sql.adaptive.skewJoin.enabled": "true",              # split skewed partitions in sort-merge joins
    "spark.sql.autoBroadcastJoinThreshold": "10MB",             # auto-broadcast tables below this size
}.items():
    print(f"{key:48s} {try_set(key, value)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: explicit broadcast

# COMMAND ----------

joined = customer.join(F.broadcast(nation), customer.c_nationkey == nation.n_nationkey)
joined.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** `BroadcastExchange HashedRelationBroadcastMode` on the `nation` side, then `BroadcastHashJoin ... BuildRight`,
# MAGIC and **no** `Exchange hashpartitioning` on the `customer` side. The top node is `AdaptiveSparkPlan isFinalPlan=false`: the plan
# MAGIC before AQE has seen runtime statistics. After running, the query profile (or Spark UI) shows the final plan.

# COMMAND ----------

# The same hint in SQL. Other hints: MERGE (sort-merge), SHUFFLE_HASH, SHUFFLE_REPLICATE_NL.
customer.createOrReplaceTempView("v_customer")
nation.createOrReplaceTempView("v_nation")
spark.sql("""
    SELECT /*+ BROADCAST(n) */ n.n_name, count(*) AS customers
    FROM v_customer c JOIN v_nation n ON c.c_nationkey = n.n_nationkey
    GROUP BY n.n_name
""").explain()

# COMMAND ----------

# Forcing a sort-merge join for comparison: both sides get an Exchange + Sort.
customer.hint("merge").join(nation.hint("merge"), customer.c_nationkey == nation.n_nationkey).explain()

# COMMAND ----------

display(joined.groupBy("n_name").count().orderBy(F.desc("count")).limit(5))   # run it, then open the query profile

# COMMAND ----------

# MAGIC %md
# MAGIC **In the query profile:** look for *AQEShuffleRead* (coalesced partitions), join nodes whose strategy changed at runtime,
# MAGIC and *skew* annotations when AQE split a partition. Broadcasting something big causes driver memory pressure and slow broadcast stages.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# supplier = spark.table("supplier")
# # TODO: join supplier to nation with a broadcast hint on nation
# # TODO: explain() and find BroadcastHashJoin
# # TODO: write the same join in SQL with /*+ BROADCAST(n) */
# # TODO: force a sort-merge join with .hint("merge") and spot the two Exchange + Sort pairs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **When NOT to broadcast?** When the "small" side is large (it can cause driver or executor OOM), or for a full outer join (not supported).
# MAGIC - **What does AQE do?** Coalesces shuffle partitions, converts sort-merge to broadcast when a side turns out small, and splits skewed partitions.
# MAGIC - **Broadcast variable vs broadcast join?** Broadcast variables (`sc.broadcast`) are an RDD feature that is unavailable on serverless; `broadcast(df)` is a join hint.
# MAGIC - **Why `isFinalPlan=false`?** `explain()` shows the plan before execution; AQE finalises it stage by stage at runtime.
