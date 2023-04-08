# Databricks notebook source
# MAGIC %md
# MAGIC ##Access Azure Data Lake using Cluster Scoped Authentication
# MAGIC 1. Set the spark Config fs.azure.account.key.
# MAGIC 2. List Files from demo container.
# MAGIC 3. Read Data from circuits.csv file

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1krdl.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1krdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.option("header",True).option("inferSchema",True).csv("abfss://demo@formula1krdl.dfs.core.windows.net/circuits.csv"))
