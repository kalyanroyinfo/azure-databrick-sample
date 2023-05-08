-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo ;

-- COMMAND ----------

show DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED f1_presentation;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE DATABASE demo;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
from demo.race_results_python
where race_year=2021;

-- COMMAND ----------

CREATE TABLE  demo.race_results_sql
AS
SELECT *
from demo.race_results_python
where race_year=2021;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py;

-- COMMAND ----------

use demo;

-- COMMAND ----------

create TABLE demo.race_results_ext_sql(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/formula1krdl1/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py where race_year=2021;

-- COMMAND ----------

select count(*) from demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES in demo;
