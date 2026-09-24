# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: copy TPC-H into the lab schema
# MAGIC **Used by:** every other notebook (run `databricks bundle run setup_tpch -t dev` first).
# MAGIC
# MAGIC **Learning objective:** create a schema idempotently and materialise tables with `CREATE OR REPLACE TABLE ... AS SELECT` (CTAS),
# MAGIC then document them with table and column comments that show up in Catalog Explorer and `information_schema`.
# MAGIC
# MAGIC **Say it out loud:** "`samples.tpch` is read-only, so the lab copies a subset into its own schema.
# MAGIC `CREATE OR REPLACE TABLE ... AS SELECT` is idempotent: re-running swaps in a new version of the Delta table in one atomic commit,
# MAGIC and the old version is still reachable through time travel."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'Databricks exam lab: TPC-H subset and exercise tables'")
spark.sql(f"USE {catalog}.{schema}")
# A managed volume for files, checkpoints and schema locations used by the streaming/ingestion labs.
spark.sql("CREATE VOLUME IF NOT EXISTS scratch COMMENT 'Checkpoints, schema locations and scratch files for the labs'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: CTAS a manageable subset
# MAGIC `samples.tpch` is sized for benchmarks. The small dimension tables are copied in full; `customer` is sampled to 1 in 25
# MAGIC and `orders` / `lineitem` keep only rows belonging to those customers, so referential integrity still holds.

# COMMAND ----------

SUBSET_MODULO = 25  # keep customers where c_custkey % 25 = 0 (~30k customers)

tables = {
    "region": "SELECT * FROM samples.tpch.region",
    "nation": "SELECT * FROM samples.tpch.nation",
    "supplier": "SELECT * FROM samples.tpch.supplier",
    "part": "SELECT * FROM samples.tpch.part",
    "partsupp": "SELECT * FROM samples.tpch.partsupp",
    "customer": f"SELECT * FROM samples.tpch.customer WHERE c_custkey % {SUBSET_MODULO} = 0",
    "orders": f"SELECT * FROM samples.tpch.orders WHERE o_custkey % {SUBSET_MODULO} = 0",
    "lineitem": f"""SELECT l.* FROM samples.tpch.lineitem l
                    JOIN samples.tpch.orders o ON l.l_orderkey = o.o_orderkey
                    WHERE o.o_custkey % {SUBSET_MODULO} = 0""",
}

table_comments = {
    "region": "TPC-H regions (5 rows), copied from samples.tpch",
    "nation": "TPC-H nations (25 rows) with n_regionkey -> region",
    "supplier": "TPC-H suppliers with s_nationkey -> nation",
    "part": "TPC-H parts catalogue",
    "partsupp": "Which supplier supplies which part, with supply cost and available quantity",
    "customer": f"TPC-H customers, 1-in-{SUBSET_MODULO} sample by c_custkey",
    "orders": "Orders placed by the sampled customers",
    "lineitem": "Line items of the sampled orders",
}

for name, select_sql in tables.items():
    # CREATE OR REPLACE keeps the job idempotent: re-runs replace the table atomically.
    spark.sql(f"CREATE OR REPLACE TABLE {name} COMMENT '{table_comments[name]}' AS {select_sql}")

# COMMAND ----------

# Column comments on the keys and measures the labs use most.
column_comments = {
    "customer": {"c_custkey": "Customer business key", "c_nationkey": "FK to nation.n_nationkey",
                 "c_acctbal": "Account balance (can be negative)", "c_mktsegment": "Market segment",
                 "c_phone": "PII: phone number", "c_address": "PII: street address"},
    "nation": {"n_nationkey": "Nation key", "n_regionkey": "FK to region.r_regionkey"},
    "supplier": {"s_suppkey": "Supplier key", "s_nationkey": "FK to nation.n_nationkey"},
    "partsupp": {"ps_partkey": "FK to part.p_partkey", "ps_suppkey": "FK to supplier.s_suppkey",
                 "ps_supplycost": "Cost of the part from this supplier", "ps_availqty": "Quantity the supplier has available"},
    "orders": {"o_orderkey": "Order key", "o_custkey": "FK to customer.c_custkey", "o_totalprice": "Order total"},
}
for table, cols in column_comments.items():
    for col, text in cols.items():
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{text}'")

# COMMAND ----------

# Verify: row counts per table. Expect region=5, nation=25 and a few hundred thousand orders.
counts = [(t, spark.table(t).count()) for t in tables]
display(spark.createDataFrame(counts, "table STRING, row_count LONG"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill
# MAGIC Retype from memory, then uncomment and run.

# COMMAND ----------

# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
# # TODO: CTAS the nation table from samples.tpch.nation with a table comment
# # TODO: add a column comment to n_regionkey
# # TODO: confirm the comment with DESCRIBE TABLE EXTENDED nation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **CTAS vs `INSERT OVERWRITE`?** CTAS (with `OR REPLACE`) can also change the schema; `INSERT OVERWRITE` keeps the existing schema and table properties.
# MAGIC - **Why not `DROP TABLE` + `CREATE`?** Drop loses history and grants and leaves a window where the table is missing; `OR REPLACE` is one atomic commit.
# MAGIC - **Where do comments live?** In the Unity Catalog metastore, so they appear in `information_schema.tables/columns` and Catalog Explorer.
# MAGIC - **Managed vs external table?** These are managed: Unity Catalog owns the files, and `DROP` removes the data too.
