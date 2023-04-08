# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest laptimes folder CSV files parse

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,TimestampType,DoubleType,FloatType
from pyspark.sql.functions import current_timestamp,col,concat,lit

# COMMAND ----------


lap_times_schema=StructType(fields=[
    StructField("raceId",IntegerType(),True),
    StructField("driverId",IntegerType(),True),
    StructField("lap",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("milliseconds",IntegerType(),True),
])

# COMMAND ----------

lap_times_df=spark.read.schema(lap_times_schema).option("header",True).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename Column

# COMMAND ----------

lap_times_final_df=lap_times_df.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("ingestion_date",current_timestamp()).withColumn("data_source",lit(v_data_source))

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")


# COMMAND ----------

display(spark.read.parquet("/mnt/formula1krdl/processed/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")
