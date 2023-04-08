# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake(managed table)
# MAGIC 2. Write data to delta lake(external table)
# MAGIC 3. Read data from delta lake(Table)
# MAGIC 3. Read data from delta lake(File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1krdl/demo/'

# COMMAND ----------

results_df=spark.read.option("inferSchema",True).json("/mnt/formula1krdl/raw/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1krdl/demo/results_external")
