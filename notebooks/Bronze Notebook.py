# Databricks notebook source
# MAGIC %md
# MAGIC ## Loading Transactions Dataset with timestamp to Maintain ingestion timestamp.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

transactions_df = spark.read.format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("/Volumes/workspace/banking_project/raw_data/transactions_enterprise.csv")

transactions_df = transactions_df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)

# COMMAND ----------

transactions_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("banking_project.bronze_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Displaying Bronze Transaction Table

# COMMAND ----------

display(transactions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Customer Dataset

# COMMAND ----------

customers_df = spark.read.format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("/Volumes/workspace/banking_project/raw_data/customers_enterprise.csv")

# COMMAND ----------

customers_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("banking_project.bronze_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Displaying Bronze Customer Table

# COMMAND ----------

display(customers_df)