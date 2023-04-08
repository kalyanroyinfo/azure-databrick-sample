# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Qualifying Folders

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,TimestampType,DoubleType,FloatType
from pyspark.sql.functions import current_timestamp,col,concat,lit

# COMMAND ----------


qualifying_schema=StructType(fields=[
    StructField("qualifyId",IntegerType(),True),
    StructField("raceId",IntegerType(),True),
    StructField("driverId",IntegerType(),True),
    StructField("constructorId",IntegerType(),True),
    StructField("number",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("q1",StringType(),True),
    StructField("q2",StringType(),True),
    StructField("q3",StringType(),True),
])

# COMMAND ----------

qualifying_df=spark.read.schema(qualifying_schema).option("multiLine",True).option("header",True).json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename Column

# COMMAND ----------

qualifying_final_df=qualifying_df.withColumnRenamed("qualifyId","qualify_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumn("ingestion_date",current_timestamp()).withColumn("data_source",lit(v_data_source))

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")


# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")
