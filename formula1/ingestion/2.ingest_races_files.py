# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest races.csv files

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType, DateType, TimestampType

# COMMAND ----------

races_schema=StructType(fields=[
    StructField("raceId",IntegerType(),True),
    StructField("year",IntegerType(),True),
    StructField("round",IntegerType(),True),
    StructField("circuitId",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("date",DateType(),True),
    StructField("time",StringType(),True),
    StructField("url",StringType(),True)
])

# COMMAND ----------

races_df=spark.read.schema(races_schema).option("header",True).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select Only Required Columns

# COMMAND ----------

from pyspark.sql.functions import col
races_selected_df=races_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))



# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename Column

# COMMAND ----------

races_renamed_df=races_selected_df.withColumnRenamed("raceId","race_id").withColumnRenamed("year","race_year").withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add Timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,lit
races_timestamp_df=races_renamed_df.withColumn("ingestion_date",current_timestamp()).withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),'yyyy-MM-dd HH:mm:ss')).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

races_final_df=races_timestamp_df.select(col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("race_timestamp"),col("ingestion_date"))
display(races_final_df)

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")


# COMMAND ----------

dbutils.notebook.exit("Success")
