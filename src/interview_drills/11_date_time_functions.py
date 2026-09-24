# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Date and time functions
# MAGIC **Related exam objectives:** Spark Developer Associate, Developing Apache Spark DataFrame/DataSet API Applications (30%); Using Spark SQL (20%)
# MAGIC
# MAGIC **Learning objective:** extract date parts, do date arithmetic, truncate for time bucketing, and parse and format strings with explicit patterns.
# MAGIC
# MAGIC **Say it out loud:** "Dates are `DATE` and timestamps are `TIMESTAMP`, and I keep them typed rather than as strings. Parsing uses `to_date` or `to_timestamp`
# MAGIC with an explicit pattern, and bucketing uses `date_trunc`. Timestamps are shown in the session time zone
# MAGIC (`spark.sql.session.timeZone`), which is the one date config serverless lets me set."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F

orders = spark.table("orders").select("o_orderkey", "o_custkey", "o_orderdate", "o_totalprice")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: parts, arithmetic and bucketing

# COMMAND ----------

display(orders.select(
    "o_orderdate",
    F.year("o_orderdate").alias("yr"),
    F.quarter("o_orderdate").alias("qtr"),
    F.month("o_orderdate").alias("mon"),
    F.dayofweek("o_orderdate").alias("dow"),                      # 1 = Sunday
    F.weekofyear("o_orderdate").alias("week"),
    F.date_add("o_orderdate", 30).alias("plus_30d"),
    F.add_months("o_orderdate", 1).alias("plus_1m"),
    F.last_day("o_orderdate").alias("month_end"),
    F.date_trunc("month", "o_orderdate").alias("month_start_ts"), # returns TIMESTAMP
    F.trunc("o_orderdate", "year").alias("year_start"),           # returns DATE
    F.datediff(F.lit("1998-12-31").cast("date"), "o_orderdate").alias("days_to_end"),
    F.date_format("o_orderdate", "yyyy-MM (EEE)").alias("formatted"),
).limit(5))

# COMMAND ----------

# Bucketing: order revenue per quarter (not a product-level query, just a time rollup).
display(
    orders.groupBy(F.date_trunc("quarter", "o_orderdate").alias("quarter"))
    .agg(F.count("*").alias("orders"), F.round(F.sum("o_totalprice"), 0).alias("revenue"))
    .orderBy("quarter")
)

# COMMAND ----------

# Parsing strings. Patterns are case sensitive: MM = month, mm = minutes.
raw = spark.createDataFrame([("03/15/1996 14:30",), ("12/01/1997 08:05",), ("bad",)], "s STRING")
display(raw.select(
    "s",
    F.try_to_timestamp("s", F.lit("MM/dd/yyyy HH:mm")).alias("ts"),   # NULL for "bad" instead of an error under ANSI mode
    F.to_date(F.try_to_timestamp("s", F.lit("MM/dd/yyyy HH:mm"))).alias("d"),
))

# COMMAND ----------

# Epoch seconds and current values; session time zone (a serverless-supported config).
spark.conf.set("spark.sql.session.timeZone", "UTC")
display(spark.range(1).select(
    F.current_date().alias("today"),
    F.current_timestamp().alias("now_utc"),
    F.unix_timestamp(F.lit("1998-08-02 00:00:00")).alias("epoch_s"),
    F.from_unixtime(F.lit(902016000)).alias("from_epoch"),
    F.months_between(F.lit("1998-08-02"), F.lit("1992-01-01")).alias("months_between"),
))

# COMMAND ----------

orders.filter(F.col("o_orderdate") >= "1998-01-01").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** the string literal is cast once to a DATE (`o_orderdate >= 1998-01-01`) and pushed down as
# MAGIC `PushedFilters: [GreaterThanOrEqual(o_orderdate,1998-01-01)]`. If you wrap the column in a function instead (e.g. `year(o_orderdate) = 1998`),
# MAGIC data skipping gets weaker, so filter on the raw column whenever you can.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# drill = orders.select(
#     "o_orderdate",
#     # TODO: year and month of o_orderdate
#     # TODO: first day of the month as a DATE (trunc)
#     # TODO: days between o_orderdate and 1998-08-02
#     # TODO: o_orderdate formatted as 'dd MMM yyyy'
# )
# # TODO: count orders per month using date_trunc('month', ...)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **`date_trunc` vs `trunc`?** `date_trunc(unit, ts)` returns a TIMESTAMP and supports hour/minute; `trunc(date, unit)` returns a DATE.
# MAGIC - **A string that doesn't match the pattern?** It errors under ANSI mode; use `try_to_timestamp` or `try_to_date` to get NULL.
# MAGIC - **Where are time zones applied?** When TIMESTAMPs are rendered or parsed (session time zone); they are stored as UTC instants.
# MAGIC - **Filter on `year(col)`?** It works, but it is less pushdown and skipping friendly than a range predicate on the raw column.
