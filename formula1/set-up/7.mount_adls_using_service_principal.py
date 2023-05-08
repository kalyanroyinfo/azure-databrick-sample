# Databricks notebook source
# MAGIC %md
# MAGIC ##Mount ADLS using Service Principal
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utlity mount to mount the storage
# MAGIC 4. Explore other file system utlities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id=dbutils.secrets.get("formula1kr1","formula1-client-id")
tenant_id=dbutils.secrets.get("formula1kr1","formula1-tenant-id")
client_secret=dbutils.secrets.get("formula1kr1","formula1-client-secret")
storage_account_name='formula1krdl1'

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("demo")
mount_adls("presentation")
mount_adls("processed")
mount_adls("raw")

# COMMAND ----------

display(dbutils.fs.ls(f"dbfs:/mnt/{storage_account_name}"))

# COMMAND ----------

display(dbutils.fs.ls(f"dbfs:/mnt/{storage_account_name}/"))

# COMMAND ----------

display(dbutils.fs.ls(f"dbfs:/mnt/{storage_account_name}/raw"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# dbutils.fs.unmount("/mnt/formula1krdl/demo")
