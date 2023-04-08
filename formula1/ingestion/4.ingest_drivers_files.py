# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Drivers.json files

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,TimestampType
from pyspark.sql.functions import current_timestamp,col,concat,lit

# COMMAND ----------

name_schema=StructType(fields=[
    StructField("forename",StringType(),True),
    StructField("surname",StringType(),True)
])
drivers_schema=StructType(fields=[
    StructField("driverId",IntegerType(),True),
    StructField("driverRef",StringType(),True),
    StructField("number",IntegerType(),True),
    StructField("code",StringType(),True),
    StructField("name",name_schema),
    StructField("dob",StringType(),True),
    StructField("nationality",StringType(),True),
    StructField("url",StringType(),True)
])

# COMMAND ----------

drivers_df=spark.read.schema(drivers_schema).option("header",True).json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select Only Required Columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename Column

# COMMAND ----------

drivers_renamed_df=drivers_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("name",concat(col("name.forename"),lit(' '),col("name.surname"))).withColumn("ingestion_date",current_timestamp()).withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

drivers_renamed_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")


# COMMAND ----------

dbutils.notebook.exit("Success")
