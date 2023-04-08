# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest circuits.csv files

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,TimestampType
from pyspark.sql.functions import lit

# COMMAND ----------

circuits_schema=StructType(fields=[
    StructField("circuitId",IntegerType(),True),
    StructField("circuitRef",StringType(),True),
    StructField("name",StringType(),True),
    StructField("location",StringType(),True),
    StructField("country",StringType(),True),
    StructField("lat",DoubleType(),True),
    StructField("lng",DoubleType(),True),
    StructField("alt",IntegerType(),True),
    StructField("url",StringType(),True)
])

# COMMAND ----------

circuits_df=spark.read.schema(circuits_schema).option("header",True).csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select Only Required Columns

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))



# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename Column

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_Ref").withColumnRenamed("lat","latitude").withColumnRenamed("lng","longitude").withColumnRenamed("alt","altitude").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add Timestamp

# COMMAND ----------



circuits_final_df=add_ingestion_date(circuits_renamed_df)
display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")
