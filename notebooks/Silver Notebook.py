# Databricks notebook source
# MAGIC %md
# MAGIC ## Loading Data from the bronze transactions and bronze customers tables

# COMMAND ----------

transactions = spark.table("banking_project.bronze_transactions")
customers = spark.table("banking_project.bronze_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Removing Duplicates

# COMMAND ----------

transactions_clean = transactions.dropDuplicates(
    ["transaction_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Null Customer IDs

# COMMAND ----------

transactions_clean = transactions_clean.filter(
    transactions_clean.customer_id.isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Customer + Transaction

# COMMAND ----------

silver_df = transactions_clean.join(
    customers,
    "customer_id",
    "left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction Category

# COMMAND ----------

from pyspark.sql.functions import when

silver_df = silver_df.withColumn(
    "transaction_category",
    when(silver_df.amount < 1000, "Small")
    .when(
        (silver_df.amount >= 1000) &
        (silver_df.amount <= 10000),
        "Medium"
    )
    .otherwise("High")
)

# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("banking_project.silver_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Displaying Final Silver Table

# COMMAND ----------

display(silver_df)