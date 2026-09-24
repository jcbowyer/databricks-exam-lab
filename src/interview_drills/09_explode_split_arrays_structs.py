# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · explode, split, arrays and structs
# MAGIC **Related exam objectives:** Spark Developer Associate, Developing Apache Spark DataFrame/DataSet API Applications (30%)
# MAGIC
# MAGIC **Learning objective:** turn strings into arrays (`split`), arrays into rows (`explode`, `posexplode`, `explode_outer`), and group fields into structs.
# MAGIC
# MAGIC **Say it out loud:** "`split` gives an array column, and `explode` turns each element into its own row, so the row count grows.
# MAGIC `explode` drops rows whose array is NULL or empty, and `explode_outer` keeps them. Structs let me nest related fields,
# MAGIC and I read them back with dot notation or `col.*`."

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "exam_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

from pyspark.sql import functions as F

part = spark.table("part")    # p_name is five colour words, e.g. "goldenrod lavender spring chocolate lace"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worked example: which colour words appear most in part names?

# COMMAND ----------

with_words = part.select("p_partkey", "p_name", F.split("p_name", " ").alias("words"))
display(with_words.limit(3))

colour_counts = (
    with_words
    .select("p_partkey", F.explode("words").alias("word"))          # one row per word
    .groupBy("word").count()
    .orderBy(F.desc("count"))
)
display(colour_counts.limit(10))

# COMMAND ----------

# Array functions (no explode needed)
display(with_words.select(
    "p_partkey",
    F.size("words").alias("n_words"),
    F.element_at("words", 1).alias("first_word"),                   # 1-based; words[0] is 0-based
    F.array_contains("words", "green").alias("has_green"),
    F.sort_array("words").alias("sorted_words"),
    F.array_join(F.slice("words", 1, 2), "-").alias("first_two"),
).limit(5))

# posexplode keeps the position; explode_outer keeps rows with NULL/empty arrays.
display(with_words.limit(2).select("p_partkey", F.posexplode("words").alias("pos", "word")))
demo = spark.createDataFrame([(1, ["a", "b"]), (2, [])], "id INT, arr ARRAY<STRING>")
print("explode rows:", demo.select(F.explode("arr")).count(), "| explode_outer rows:", demo.select(F.explode_outer("arr")).count())

# COMMAND ----------

# Structs: pack related columns, then read them back.
packed = spark.table("supplier").select(
    "s_suppkey",
    F.struct("s_name", "s_phone", F.col("s_acctbal").alias("balance")).alias("info"),
)
packed.printSchema()
display(packed.select("s_suppkey", "info.s_name", "info.balance").limit(3))   # dot access
display(packed.select("s_suppkey", "info.*").limit(3))                        # flatten

# Array of structs: collect each nation's suppliers, then explode back.
nested = packed.join(spark.table("supplier").select("s_suppkey", "s_nationkey"), "s_suppkey") \
    .groupBy("s_nationkey").agg(F.collect_list("info").alias("suppliers"))
display(nested.select("s_nationkey", F.size("suppliers").alias("n")).orderBy("s_nationkey").limit(5))

# COMMAND ----------

colour_counts.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What to look for:** a `Generate explode(split(p_name, ...))` node, which is a narrow operator that multiplies rows, followed by the usual aggregate + `Exchange`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill

# COMMAND ----------

# # p_type looks like "STANDARD POLISHED BRASS". Count parts per last word (the metal).
# drill = (part
#     # TODO: split p_type on space into type_words
#     # TODO: take the last element with element_at(type_words, -1) as metal
#     # TODO: groupBy metal, count, order desc
# )
# # TODO: explode type_words and count distinct words overall

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview follow-ups
# MAGIC - **`explode` vs `explode_outer`?** `explode` drops NULL or empty arrays; `explode_outer` keeps the row and returns NULL.
# MAGIC - **Can you have two explodes in one `select`?** No, only one generator per select. Chain selects, or use `arrays_zip` and then explode.
# MAGIC - **Exploding a map?** `explode(map)` gives `key` and `value` columns.
# MAGIC - **Reverse of explode?** `groupBy` + `collect_list` (keeps duplicates) or `collect_set` (distinct).
