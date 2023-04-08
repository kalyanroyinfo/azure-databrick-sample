# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_filtered_df=races_df.filter("race_year=2019")

# COMMAND ----------

display(races_filtered_df)
