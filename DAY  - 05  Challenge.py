# Databricks notebook source
# Create Spark DataFrames
updates = spark.createDataFrame([ (1655, "2-21-2023", 79, 521), 
                                 (1665, "2-21-2023", 14, 594),
                                 (1685, "2-22-2023", 21, 646),
                                 (1705, "2-22-2023", 19, 594)],
                                ["order_id", "date", "customer_id", "product_id"])


# COMMAND ----------

# Task 01:- Merge the updates DataFrame with the Delta table

from pyspark.sql.functions import to_date

# Convert date column to date type
updates = updates.withColumn("date", to_date("date", "M-d-yyyy"))

delta_tbl = DeltaTable.forName(spark, "`sale_data")

delta_tbl.alias("t").merge(
    updates.alias("s"),
    "t.order_id = s.order_id"
).whenMatchedUpdate(
    set={"product_id": "s.product_id"}
).whenNotMatchedInsert(
    values={
        "order_id": "s.order_id",
        "date": "s.date",
        "customer_id": "s.customer_id",
        "product_id": "s.product_id"
    }
).execute()

# COMMAND ----------

# Task 02:- Display the history of the Delta table
display(spark.sql("DESCRIBE HISTORY workspace.default.sale_data"))

# Read the Delta table at version 0
old_version = spark.read.format("delta")\
    .option("versionAsOf", 0)\
    .table("workspace.default.sale_data")

display(old_version.limit(5))

# COMMAND ----------

# Task 03:- Optimize the Delta table
spark.sql("""
OPTIMIZE workspace.default.sale_data 
ZORDER BY (date)
""")

# COMMAND ----------

# Task 04:- Clean old files 
spark.sql("VACUUM workspace.default.sale_data RETAIN 168 HOURS")