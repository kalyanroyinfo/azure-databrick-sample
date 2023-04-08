# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df=race_results_df.filter("race_year==2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct
demo_df.groupBy("driver_name").agg(sum("points"),countDistinct("race_name")).show()

# COMMAND ----------

demo_df1=race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df1)

# COMMAND ----------

demo_grouped_df=demo_df.groupBy("race_year","driver_name").sum("points").withColumnRenamed("sum(points)","total_points")
display(demo_grouped_df)
