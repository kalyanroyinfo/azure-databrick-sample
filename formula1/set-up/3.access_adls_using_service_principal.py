# Databricks notebook source
# MAGIC %md
# MAGIC ##Access Azure Data Lake using Service Principal
# MAGIC 1. Set the spark Config service principal
# MAGIC 2. List Files from demo container.
# MAGIC 3. Read Data from circuits.csv file

# COMMAND ----------

client_id=dbutils.secrets.get("formula1kr1","formula1-client-id")
tenant_id=dbutils.secrets.get("formula1kr1","formula1-tenant-id")
client_secret=dbutils.secrets.get("formula1kr1","formula1-client-secret")
storage_account='formula1krdl1'

# COMMAND ----------

# client_id='1f8a0b94-c784-42b2-a4b6-64b7ec12e755'
# tenant_id='4066fbb7-3e81-4f70-a519-36b43ed0d4a2'
# client_secret='aaQ8Q~cO~hQWGgpMljriejUBO3FPkpgVUHMpdai_'
# storage_account='formula1krdl1'

# COMMAND ----------



spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls(f"abfss://raw@{storage_account}.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://raw@{storage_account}.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv(f"abfss://raw@{storage_account}.dfs.core.windows.net/circuits.csv"))
