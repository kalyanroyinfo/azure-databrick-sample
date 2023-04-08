# Databricks notebook source
# MAGIC %md
# MAGIC ##Access Azure Data Lake using Service Principal
# MAGIC 1. Set the spark Config service principal
# MAGIC 2. List Files from demo container.
# MAGIC 3. Read Data from circuits.csv file

# COMMAND ----------

client_id=dbutils.secrets.get("formula1-scope","formula1-client-id")
tenant_id=dbutils.secrets.get("formula1-scope","formula1-tenant-id")
client_secret=dbutils.secrets.get("formula1-scope","formula1-client-secret")

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.formula1krdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1krdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1krdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1krdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1krdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1krdl.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1krdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1krdl.dfs.core.windows.net/circuits.csv"))
