# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Window: lag / lead and running totals
# MAGIC **Related exam objectives:** Spark Developer Associate, Developing Apache Spark DataFrame/DataSet API Applications (30%)
# MAGIC
# MAGIC **Learning objective:** compare each row with its neighbours (`lag`, `lead`) and compute running and moving aggregates with explicit frame specs.
# MAGIC
# MAGIC **Say it out loud:** "`lag` and `lead` look a fixed number of rows back or forward within the partition. For running totals,
# MAGIC I set the frame explicitly with `rowsBetween(unboundedPreceding, currentRow)`. The default frame with an `orderBy` is RANGE-based,
# MAGIC so rows with the same ordering value are summed together, which surprises people."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import Window
from pyspark.sql import functions as F

# A handful of customers keeps the output readable.
orders = spark.table("orders").filter(F.col("o_custkey").isin(25, 50, 100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: order-to-order gaps and cumulative spend per customer

# COMMAND ----------

ordered = Window.partitionBy("o_custkey").orderBy("o_orderdate", "o_orderkey")
running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
last_3 = ordered.rowsBetween(-2, Window.currentRow)            # moving window: this row + 2 before

history = orders.select(
    "o_custkey", "o_orderkey", "o_orderdate", "o_totalprice",
    F.lag("o_orderdate").over(ordered).alias("prev_order_date"),
    F.lead("o_orderdate").over(ordered).alias("next_order_date"),
    F.datediff("o_orderdate", F.lag("o_orderdate").over(ordered)).alias("days_since_prev"),
    F.round(F.sum("o_totalprice").over(running), 2).alias("running_spend"),
    F.round(F.avg("o_totalprice").over(last_3), 2).alias("moving_avg_3"),
    F.first("o_orderdate").over(ordered).alias("first_order_date"),
)
display(history.orderBy("o_custkey", "o_orderdate"))

# COMMAND ----------

# ROWS vs RANGE: when ordering values tie, the default RANGE frame includes all peer rows.
demo = spark.createDataFrame(
    [(1, "2024-01-01", 10), (1, "2024-01-01", 20), (1, "2024-01-02", 5)], "k INT, d STRING, amt INT"
)
w = Window.partitionBy("k").orderBy("d")
display(demo.select(
    "d", "amt",
    F.sum("amt").over(w).alias("default_range_frame"),               # 30, 30, 35
    F.sum("amt").over(w.rowsBetween(Window.unboundedPreceding, 0)).alias("rows_frame"),  # 10 or 20, 30, 35
))

# COMMAND ----------

# Share of the customer's total spend: an unordered window means the whole partition.
display(orders.select(
    "o_custkey", "o_orderkey",
    F.round(F.col("o_totalprice") / F.sum("o_totalprice").over(Window.partitionBy("o_custkey")) * 100, 1).alias("pct_of_customer"),
).orderBy("o_custkey", F.desc("pct_of_customer")).limit(10))

# COMMAND ----------

history.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** a single `Window` node (or a few stacked ones) computing `lag`, `lead`, `sum ... specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())`.
# MAGIC Windows that share the same partition and order spec share one `Exchange` + `Sort`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# w = (Window
#      # TODO: partition by o_custkey, order by o_orderdate, o_orderkey
#     )
# drill = orders.select(
#     "o_custkey", "o_orderdate", "o_totalprice",
#     # TODO: previous order's totalprice via lag
#     # TODO: change vs previous order (o_totalprice - lag)
#     # TODO: running total with rowsBetween(unboundedPreceding, currentRow)
# )
# display(drill)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **Default frame with an `orderBy`?** RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, so ties are summed together.
# MAGIC - **Default frame without an `orderBy`?** The entire partition.
# MAGIC - **`lag` on the first row?** NULL, unless you pass a default: `lag(col, 1, 0)`.
# MAGIC - **A window with no `partitionBy`?** All rows go to one partition, which triggers a warning and a single-task bottleneck.
