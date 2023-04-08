# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum,when,col,count
constructors_standings_df=race_results_df.groupBy("race_year","team").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(constructors_standings_df.filter("race_year==2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank,asc

constructors_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),asc("wins"))
final_df=constructors_standings_df.withColumn("rank",rank().over(constructors_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year==2020"))

# COMMAND ----------

final_df.write.parquet(f"{presentation_folder_path}/constructors_standings")
