# Databricks notebook source
# MAGIC %md
# MAGIC ##Access Azure Data Lake using keys
# MAGIC 1. Set the spark Config fs.azure.account.key.
# MAGIC 2. List Files from demo container.
# MAGIC 3. Read Data from circuits.csv file

# COMMAND ----------



# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1krdl.dfs.core.windows.net",dbutils.secrets.get(scope="formula1-scope",key="formula1dl-account-key"))

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1krdl.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1krdl.dfs.core.windows.net/circuits.csv"))
