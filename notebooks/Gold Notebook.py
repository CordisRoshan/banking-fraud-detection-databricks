# Databricks notebook source
# MAGIC %md
# MAGIC ## Reading Data from Silver Notebook Output

# COMMAND ----------

from pyspark.sql.functions import *

silver = spark.table(
    "banking_project.silver_transactions"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Transaction Summary

# COMMAND ----------

customer_summary = silver.groupBy(
    "customer_id"
).agg(
    count("transaction_id").alias(
        "total_transactions"
    ),
    sum("amount").alias(
        "total_amount"
    ),
    avg("amount").alias(
        "avg_transaction_amount"
    )
)

# COMMAND ----------

customer_summary.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "banking_project.gold_customer_summary"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Displaying Gold Summary Table

# COMMAND ----------

display(customer_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud Detection Table
# MAGIC

# COMMAND ----------

daily_summary = silver.groupBy(
    "customer_id",
    "transaction_date"
).agg(
    count("*").alias("txn_count"),
    sum("amount").alias("daily_amount")
)

# COMMAND ----------

fraud_df = daily_summary.filter(
    (col("txn_count") > 15)
    |
    (col("daily_amount") > 175000)
)

# COMMAND ----------

fraud_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "banking_project.gold_fraud_candidates"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Displaying Gold Fraud Customers

# COMMAND ----------

display(fraud_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Additional Requirements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

# COMMAND ----------

city_spend = silver.groupBy(
    "city",
    "customer_id",
    "customer_name"
).agg(
    sum("amount").alias("total_spend")
)

# COMMAND ----------

city_spend.head(5)

# COMMAND ----------

window_spec = Window.partitionBy(
    "city"
).orderBy(
    col("total_spend").desc()
)

# COMMAND ----------

top5 = city_spend.withColumn(
    "rank",
    dense_rank().over(window_spec)
)

display(top5.filter("rank<=5"))