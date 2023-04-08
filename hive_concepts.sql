-- Databricks notebook source
-- DBTITLE 1,Hive Concepts
-- MAGIC %md
-- MAGIC 
-- MAGIC 1. Understand the hive concepts in databricks
-- MAGIC 2. Database objects
-- MAGIC 3. Hive managed tables
-- MAGIC 4. Hive external tables
-- MAGIC 5. Create views

-- COMMAND ----------

show databases

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS lending_loan_tmp;

-- COMMAND ----------

show databases

-- COMMAND ----------

DESCRIBE DATABASE lending_loan_tmp;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS lending_loan_dev LOCATION '/mnt/extdbstore/cleaned-data/'

-- COMMAND ----------

show databases

-- COMMAND ----------

DESCRIBE DATABASE lending_loan_dev

-- COMMAND ----------

use default

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val cleanedScript_folder_path="/Users/kalyanroyinfo@gmail.com/Pipeline_Script/DataCleaning/"
-- MAGIC val processed_file_path="/mnt/extdbstore/processed-data/lending_loan/"
-- MAGIC val cleaned_file_path="/mnt/extdbstore/cleaned-data/lending_loan/"

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val customer_df = spark.read.parquet(processed_file_path+"customer_details")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customer_df.write.format("parquet").saveAsTable("lending_loan_tmp.customer_details")

-- COMMAND ----------

SHOW TABLES IN lending_loan_tmp;

-- COMMAND ----------

select * from lending_loan_tmp.customer_details;

-- COMMAND ----------

CREATE OR REPLACE TABLE lending_loan_tmp.customer_genz as
SELECT * FROM lending_loan_tmp.customer_details WHERE age between 18 and 45

-- COMMAND ----------

SELECT * from lending_loan_tmp.customer_genz;

-- COMMAND ----------

SHOW TABLEs in lending_loan_tmp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Hive External Tables
-- MAGIC 1. Create external hive tables
-- MAGIC 2. View the externally stored data
-- MAGIC 3. Drop external tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS lending_loan_dev LOCATION "cleaned_file_path"

-- COMMAND ----------

USE lending_loan_dev;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val customer_df_external = spark.read.parquet(processed_file_path+"customer_details")
-- MAGIC customer_df_external.write.format("parquet").saveAsTable("lending_loan_dev.customer_details_temp")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC DATABASE EXTENDED lending_loan_dev;

-- COMMAND ----------

DESC EXTENDED lending_loan_dev.customer_details_temp;

-- COMMAND ----------

CREATE EXTERNAL TABLE lending_loan_tmp.customer_details_external1
(
customer_key STRING,
ingest_date TIMESTAMP,
customer_id STRING,
member_id STRING,
first_name STRING,
last_name STRING,
premium_status STRING ,
age INT,
state STRING,
country STRING
)
USING PARQUET
LOCATION "/mnt/extdbstore/cleaned-data/lending_loan/customer_details_external"

-- COMMAND ----------

INSERT INTO lending_loan_tmp.customer_details_external1
select * from lending_loan_dev.customer_details_temp

-- COMMAND ----------

DESC EXTENDED lending_loan_tmp.customer_details_external1
