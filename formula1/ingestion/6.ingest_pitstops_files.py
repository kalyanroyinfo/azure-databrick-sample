# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Results.json files

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,TimestampType,DoubleType,FloatType
from pyspark.sql.functions import current_timestamp,col,concat,lit

# COMMAND ----------


pit_stops_schema=StructType(fields=[
    StructField("raceId",IntegerType(),True),
    StructField("driverId",IntegerType(),True),
    StructField("stop",IntegerType(),True),
    StructField("lap",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("duration",StringType(),True),
    StructField("milliseconds",IntegerType(),True),
])

# COMMAND ----------

pit_stops_df=spark.read.schema(pit_stops_schema).option("multiLine",True).option("header",True).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename Column

# COMMAND ----------

pit_stops_final_df=pit_stops_df.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("ingestion_date",current_timestamp()).withColumn("data_source",lit(v_data_source))

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

