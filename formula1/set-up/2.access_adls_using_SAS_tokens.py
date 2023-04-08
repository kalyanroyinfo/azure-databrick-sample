# Databricks notebook source
# MAGIC %md
# MAGIC ##Access Azure Data Lake SAS tokens
# MAGIC 1. Set the spark Config SAS token
# MAGIC 2. List Files from demo container.
# MAGIC 3. Read Data from circuits.csv file

# COMMAND ----------

sas_token = dbutils.secrets.get("formula1-scope","formula1-SAS-token-latest")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1krdl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1krdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1krdl.dfs.core.windows.net", sas_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1krdl.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1krdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1krdl.dfs.core.windows.net/circuits.csv"))
