# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Constructors.json files

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit

# COMMAND ----------

constructors_schema="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructors_df=spark.read.schema(constructors_schema).option("header",True).json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select Only Required Columns

# COMMAND ----------

constructor_dropped_df=constructors_df.drop(col("url"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename Column

# COMMAND ----------

constructor_final_df=constructor_dropped_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("ingestion_date",current_timestamp()).withColumn("data_source",lit(v_data_source))

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")


# COMMAND ----------

dbutils.notebook.exit("Success")
