# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Read with an explicit schema, write Delta
# MAGIC **Related exam objectives:** Spark Developer Associate, Developing Apache Spark DataFrame/DataSet API Applications (30%); Data Engineer Professional, Data Ingestion & Acquisition (7%)
# MAGIC
# MAGIC **Learning objective:** read CSV files with a declared `StructType` (and the DDL-string equivalent), choose a parse mode,
# MAGIC and write the result as a managed Delta table.
# MAGIC
# MAGIC **Say it out loud:** "Schema inference makes an extra pass over the data and can guess the wrong types. In production I
# MAGIC declare the schema, so reads are faster and type drift fails loudly instead of silently. I then write to Delta, which
# MAGIC gives ACID commits, schema enforcement and time travel on top of Parquet."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType, StringType, StructField, StructType

csv_path = f"/Volumes/{catalog}/{schema}/scratch/interview_drills/customer_csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare input files
# MAGIC Write a CSV extract of `customer` into the `scratch` volume so there are real files to read.

# COMMAND ----------

(spark.table("customer")
    .limit(5000)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(csv_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: explicit `StructType`

# COMMAND ----------

customer_schema = StructType([
    StructField("c_custkey", LongType(), nullable=False),
    StructField("c_name", StringType()),
    StructField("c_address", StringType()),
    StructField("c_nationkey", IntegerType()),
    StructField("c_phone", StringType()),
    StructField("c_acctbal", DecimalType(18, 2)),
    StructField("c_mktsegment", StringType()),
    StructField("c_comment", StringType()),
])

customers = (
    spark.read.format("csv")
    .schema(customer_schema)          # no inference pass
    .option("header", "true")
    .option("mode", "FAILFAST")        # PERMISSIVE (default) | DROPMALFORMED | FAILFAST
    .load(csv_path)
)
customers.printSchema()

# COMMAND ----------

# Same schema as a DDL string: shorter, and handy in SQL and in read_files().
ddl = "c_custkey BIGINT, c_name STRING, c_address STRING, c_nationkey INT, c_phone STRING, c_acctbal DECIMAL(18,2), c_mktsegment STRING, c_comment STRING"
customers_ddl = spark.read.schema(ddl).option("header", "true").csv(csv_path)
assert customers_ddl.schema.simpleString() == customers.schema.simpleString()

# COMMAND ----------

# Write as a managed Delta table. saveAsTable registers it in Unity Catalog.
(customers.write
    .format("delta")            # Delta is the default on Databricks; shown for clarity
    .mode("overwrite")          # append | overwrite | error(ifexists) | ignore
    .option("overwriteSchema", "true")
    .saveAsTable("drill_customer_typed"))

display(spark.sql("DESCRIBE HISTORY drill_customer_typed").select("version", "operation", "operationMetrics"))

# COMMAND ----------

customers.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for in the plan:** `FileScan csv` with `ReadSchema: struct<c_custkey:bigint,...>`, which is the declared schema, not inferred strings.
# MAGIC If you had selected fewer columns, `ReadSchema` would shrink (column pruning).

# COMMAND ----------

# Schema enforcement: appending a DataFrame with an incompatible type fails instead of corrupting the table.
bad = spark.createDataFrame([("not-a-number",)], "c_custkey STRING")
try:
    bad.write.mode("append").saveAsTable("drill_customer_typed")
except Exception as e:
    print("Rejected by Delta schema enforcement:", type(e).__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill
# MAGIC Retype from memory, then uncomment and run.

# COMMAND ----------

# drill_schema = StructType([
#     # TODO: c_custkey BIGINT not null, c_name STRING, c_nationkey INT, c_acctbal DECIMAL(18,2)
# ])
# drill_df = (spark.read
#     # TODO: format csv, apply drill_schema, header true, mode FAILFAST
#     .load(csv_path))
# # TODO: write drill_df as Delta table drill_customer_practice with mode overwrite

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **Why not `inferSchema`?** It adds an extra pass over the data, is non-deterministic across files, and turns type drift into silent changes.
# MAGIC - **PERMISSIVE vs DROPMALFORMED vs FAILFAST?** PERMISSIVE nulls bad fields (and can use `_corrupt_record`), DROPMALFORMED skips the row, and FAILFAST throws.
# MAGIC - **`saveAsTable` vs `save(path)`?** `saveAsTable` registers a managed UC table; `save(path)` only writes files (an external location).
# MAGIC - **How do you add a column?** `.option("mergeSchema", "true")` on append, or `ALTER TABLE ADD COLUMN`.
# MAGIC - **`overwriteSchema`?** It allows `mode("overwrite")` to replace the table schema as well as the data.
