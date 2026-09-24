# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · MERGE INTO upsert (Delta)
# MAGIC **Related exam objectives:** Data Engineer Professional, Developing Code for Data Processing using Python and SQL (22%); Data Modelling (6%)
# MAGIC
# MAGIC **Learning objective:** upsert a batch of changes into a Delta table with `MERGE INTO` (SQL) and the `DeltaTable` Python API, and read the operation metrics.
# MAGIC
# MAGIC **Say it out loud:** "MERGE joins the source to the target on a key, then applies clauses: update when matched, insert when not matched,
# MAGIC and optionally delete when not matched by source. It is one atomic Delta commit. The source must have at most one row per key,
# MAGIC otherwise MERGE fails with 'multiple source rows matched', so I de-duplicate the source first."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F

# Fresh target each run so the notebook is idempotent.
spark.sql("""
    CREATE OR REPLACE TABLE drill_customer_dim AS
    SELECT c_custkey, c_name, c_nationkey, c_acctbal, c_mktsegment, current_timestamp() AS updated_at
    FROM customer WHERE c_custkey <= 5000
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a change batch: 3 updates + 2 new customers

# COMMAND ----------

updates = spark.sql("SELECT c_custkey, c_name, c_nationkey, c_acctbal + 1000 AS c_acctbal, c_mktsegment FROM drill_customer_dim ORDER BY c_custkey LIMIT 3")
new_rows = spark.createDataFrame(
    [(9_000_001, "Customer#new1", 7, 1500.00, "BUILDING"), (9_000_002, "Customer#new2", 3, 250.00, "HOUSEHOLD")],
    "c_custkey BIGINT, c_name STRING, c_nationkey BIGINT, c_acctbal DOUBLE, c_mktsegment STRING",
).withColumn("c_acctbal", F.col("c_acctbal").cast("decimal(18,2)"))   # Python floats can't go straight into DECIMAL
changes = updates.unionByName(new_rows.select(updates.columns), allowMissingColumns=False)
changes.createOrReplaceTempView("v_customer_changes")
display(changes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: SQL MERGE

# COMMAND ----------

result = spark.sql("""
    MERGE INTO drill_customer_dim AS t
    USING v_customer_changes AS s
    ON t.c_custkey = s.c_custkey
    WHEN MATCHED AND t.c_acctbal <> s.c_acctbal THEN
      UPDATE SET t.c_acctbal = s.c_acctbal, t.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN
      INSERT (c_custkey, c_name, c_nationkey, c_acctbal, c_mktsegment, updated_at)
      VALUES (s.c_custkey, s.c_name, s.c_nationkey, s.c_acctbal, s.c_mktsegment, current_timestamp())
""")
display(result)   # num_affected_rows, num_updated_rows, num_deleted_rows, num_inserted_rows

# COMMAND ----------

# Re-running the same MERGE is a no-op for updates (the condition fails) and inserts (the keys now exist): it is idempotent.
display(spark.sql("DESCRIBE HISTORY drill_customer_dim").select("version", "operation", "operationMetrics").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Python API: `DeltaTable.merge`
# MAGIC Works on serverless through Delta Connect. It is wrapped in `try/except` in case your runtime's client lacks it.

# COMMAND ----------

try:
    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, "drill_customer_dim")
    (target.alias("t")
        .merge(changes.alias("s"), "t.c_custkey = s.c_custkey")
        .whenMatchedUpdate(condition="t.c_acctbal <> s.c_acctbal",
                           set={"c_acctbal": "s.c_acctbal", "updated_at": "current_timestamp()"})
        .whenNotMatchedInsert(values={"c_custkey": "s.c_custkey", "c_name": "s.c_name", "c_nationkey": "s.c_nationkey",
                                      "c_acctbal": "s.c_acctbal", "c_mktsegment": "s.c_mktsegment",
                                      "updated_at": "current_timestamp()"})
        .execute())
    print("DeltaTable.merge ran; history version:", target.history(1).select("version").first()[0])
except Exception as e:
    print("DeltaTable API not available here, use SQL MERGE:", type(e).__name__, str(e)[:200])

# COMMAND ----------

# MERGE is a command, so EXPLAIN shows little; the join it runs looks like this:
spark.table("drill_customer_dim").alias("t").join(changes.alias("s"), "c_custkey", "right").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** MERGE is planned as a join between target and source (often a broadcast of the small source), followed by a rewrite
# MAGIC of only the files that contain matched rows. The `operationMetrics` in history (`numTargetFilesAdded`, `numTargetRowsUpdated`) show how much was rewritten.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# spark.sql("""
#     MERGE INTO drill_customer_dim AS t
#     USING v_customer_changes AS s
#     -- TODO: ON the business key
#     -- TODO: WHEN MATCHED -> update balance and updated_at
#     -- TODO: WHEN NOT MATCHED -> insert all columns
#     -- TODO (stretch): WHEN NOT MATCHED BY SOURCE AND t.c_custkey > 9000000 -> DELETE
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **"Multiple source rows matched"?** The source has duplicate keys; de-duplicate it (row_number, latest per key) before MERGE.
# MAGIC - **`UPDATE SET *` / `INSERT *`?** They map columns by name; they need compatible schemas (with schema evolution: `WITH SCHEMA EVOLUTION`).
# MAGIC - **How do you make MERGE cheaper?** Add partition or cluster-key predicates to the `ON` clause so fewer target files are scanned; deletion vectors reduce rewrites.
# MAGIC - **Delete rows missing from the source?** `WHEN NOT MATCHED BY SOURCE THEN DELETE`.
