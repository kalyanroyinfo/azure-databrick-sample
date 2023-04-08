# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races").filter("race_year=2019")

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"inner")

# COMMAND ----------

display(races_circuits_df)
