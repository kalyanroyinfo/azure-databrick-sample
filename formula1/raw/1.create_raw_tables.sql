-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create Circuits Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt DOUBLE,
  url STRING
)
using csv
OPTIONS (path "/mnt/formula1krdl1/raw/circuits.csv", header true) 

-- COMMAND ----------

DESC EXTENDED f1_raw.circuits;

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create Races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
using csv
OPTIONS (path "/mnt/formula1krdl1/raw/races.csv", header true) 

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create Table from JSON Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Constructor Table
-- MAGIC ####Single Line JSON
-- MAGIC ####Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
using json
OPTIONS (path "/mnt/formula1krdl1/raw/constructors.json") 

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create drivers Table
-- MAGIC ###Single Line JSON
-- MAGIC ###Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename:STRING,surname:STRING>,
dob STRING,
nationality STRING,
url STRING
)
using json
OPTIONS (path "/mnt/formula1krdl1/raw/drivers.json") 

-- COMMAND ----------

select * from  f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create results Table
-- MAGIC ###Single Line JSON
-- MAGIC ###Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId INT
)
using json
OPTIONS (path "/mnt/formula1krdl1/raw/results.json") 

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create pit stops Table
-- MAGIC ###Multi Line JSON
-- MAGIC ###Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
raceId INT,
driverId INT,
stop INT,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
using json
OPTIONS (path "/mnt/formula1krdl1/raw/pit_stops.json", multiLine true) 

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Lap Times Table
-- MAGIC ###CSV File
-- MAGIC ###Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
using CSV
OPTIONS (path "/mnt/formula1krdl1/raw/lap_times") 

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Qualifying Table
-- MAGIC ###JSON Files
-- MAGIC ###Multiline JSON
-- MAGIC ### Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
)
using JSON
OPTIONS (path "/mnt/formula1krdl1/raw/qualifying",multiLine true) 

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;
