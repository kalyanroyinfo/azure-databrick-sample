# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Results.json files

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,TimestampType,DoubleType,FloatType
from pyspark.sql.functions import current_timestamp,col,concat,lit

# COMMAND ----------


results_schema=StructType(fields=[
    StructField("resultId",IntegerType(),True),
    StructField("raceId",IntegerType(),True),
    StructField("driverId",IntegerType(),True),
    StructField("constructorId",IntegerType(),True),
    StructField("number",IntegerType(),True),
    StructField("grid",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("positionText",StringType(),True),
    StructField("positionOrder",IntegerType(),True),
    StructField("points",FloatType(),True),
    StructField("laps",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("milliseconds",IntegerType(),True),
    StructField("fastestLap",IntegerType(),True),
    StructField("rank",IntegerType(),True),
    StructField("fastestLapTime",StringType(),True),
    StructField("fastestLapSpeed",FloatType(),True),
    StructField("statusId",IntegerType(),True),
])

# COMMAND ----------

results_df=spark.read.schema(results_schema).option("header",True).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select Only Required Columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename Column

# COMMAND ----------

results_renamed_df=results_df.withColumnRenamed("resultId","result_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("positionText","position_text").withColumnRenamed("positionOrder","position_order").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").withColumn("ingestion_date",current_timestamp()).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

results_final_df=results_renamed_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database();

# COMMAND ----------

for race_id_list in results_final_df.select("race_id").distinct().collect():
    if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION(race_id={race_id_list.race_id})")

# COMMAND ----------

results_final_df.write.mode("append").partitionBy("race_id").parquet(f"{processed_folder_path}/results")


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

df =spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

df.createOrReplaceTempView("results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(*) from results group by race_id;
