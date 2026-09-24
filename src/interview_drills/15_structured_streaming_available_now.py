# Databricks notebook source
# MAGIC %md
# MAGIC # 15 · Structured Streaming with an availableNow trigger
# MAGIC **Related exam objectives:** Spark Developer Associate, Structured Streaming (10%); Data Engineer Professional, Data Ingestion & Acquisition (7%)
# MAGIC
# MAGIC **Learning objective:** stream incrementally from a Delta table into a Delta table using `trigger(availableNow=True)` and a checkpoint,
# MAGIC and show that a rerun processes only new data.
# MAGIC
# MAGIC **Say it out loud:** "The checkpoint records which source versions and offsets have been committed. With `availableNow`, the query processes
# MAGIC everything new since the last checkpoint, possibly in several micro-batches, and then stops. That gives me incremental batch runs on a schedule
# MAGIC with exactly-once guarantees into Delta. Serverless supports only `availableNow` and `once` style triggers, not a continuous processing-time loop."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F

checkpoint = f"/Volumes/{catalog}/{schema}/scratch/checkpoints/drill_stream_sink"

# Reset so the demo is repeatable: drop source/sink and remove the checkpoint.
spark.sql("DROP TABLE IF EXISTS drill_stream_source")
spark.sql("DROP TABLE IF EXISTS drill_stream_sink")
dbutils.fs.rm(checkpoint, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: a small Delta table that receives appends ("new customers arriving")

# COMMAND ----------

def append_batch(lo, hi):
    """Append customers with lo <= c_custkey < hi to the source table as one commit."""
    (spark.table("customer")
        .filter((F.col("c_custkey") >= lo) & (F.col("c_custkey") < hi))
        .select("c_custkey", "c_name", "c_nationkey", "c_acctbal", F.current_timestamp().alias("arrived_at"))
        .write.mode("append").saveAsTable("drill_stream_source"))

append_batch(0, 10_000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: incremental stream into a sink table

# COMMAND ----------

def run_incremental():
    stream = (
        spark.readStream.table("drill_stream_source")          # Delta table as a streaming source
        .withColumn("balance_band", F.when(F.col("c_acctbal") < 0, "negative").otherwise("positive"))
    )
    query = (
        stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)           # where offsets + commits live; one per query
        .trigger(availableNow=True)                         # process everything available, then stop
        .toTable("drill_stream_sink")
    )
    query.awaitTermination()
    # availableNow may use several micro-batches, so sum them all.
    return sum(p["numInputRows"] for p in query.recentProgress)

print("run 1 rows processed:", run_incremental())
print("sink rows:", spark.table("drill_stream_sink").count())

# COMMAND ----------

# New data arrives; the rerun picks up ONLY the new commit because the checkpoint remembers the last version processed.
append_batch(10_000, 20_000)
print("run 2 rows processed:", run_incremental())
print("run 3 (nothing new):", run_incremental())
print("sink rows:", spark.table("drill_stream_sink").count(), "== source rows:", spark.table("drill_stream_source").count())

# COMMAND ----------

# The checkpoint on disk: offsets/ (what was planned), commits/ (what finished), metadata (query id).
display(dbutils.fs.ls(checkpoint))

# COMMAND ----------

spark.readStream.table("drill_stream_source").withColumn("x", F.lit(1)).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** the plan has a `StreamingRelation` / Delta streaming scan source instead of a `FileScan`. Everything else in the
# MAGIC plan looks just like batch. That is the point of Structured Streaming: same DataFrame API, run incrementally.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# drill_query = (spark.readStream
#     # TODO: read drill_stream_source as a stream
#     .writeStream
#     # TODO: delta format, append mode, checkpointLocation under the scratch volume (a NEW path)
#     # TODO: trigger availableNow
#     .toTable("drill_stream_sink_practice"))
# # TODO: awaitTermination() and print the total numInputRows from recentProgress

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **`availableNow` vs `once`?** Both stop when done; `availableNow` can split the backlog into several micro-batches (respecting rate limits), while `once` does one batch.
# MAGIC - **Can two queries share a checkpoint?** No, one checkpoint per query. Sharing corrupts progress tracking.
# MAGIC - **What if the source table is updated or deleted, not just appended?** The stream fails by default; use `skipChangeCommits` or read the change data feed.
# MAGIC - **Why a checkpoint in a UC volume?** Serverless has no DBFS root access; volumes are the governed place for files.
