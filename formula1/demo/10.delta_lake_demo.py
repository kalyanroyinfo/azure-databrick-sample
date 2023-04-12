# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake(managed table)
# MAGIC 2. Write data to delta lake(external table)
# MAGIC 3. Read data from delta lake(Table)
# MAGIC 3. Read data from delta lake(File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1krdl/demo/'

# COMMAND ----------

results_df=spark.read.option("inferSchema",True).json("/mnt/formula1krdl/raw/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1krdl/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1krdl/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

results_external_df=spark.read.format("delta").load("/mnt/formula1krdl/demo/results_external")
display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Lake
# MAGIC 2. Delete Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points=11-position
# MAGIC   WHERE position <=10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1krdl/demo/results_external')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10",
  set = { "points" : "21-position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed WHERE position >10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1krdl/demo/results_external')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("points=0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

# MAGIC %md
# MAGIC ##Upsert Using merge

# COMMAND ----------

drivers_day1_df=spark.read.option("inferSchema",True).json("/mnt/formula1krdl/raw/drivers.json").filter("driverId<=10").select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df=spark.read.option("inferSchema",True).json("/mnt/formula1krdl/raw/drivers.json").filter("driverId BETWEEN 6 AND 15").select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df=spark.read.option("inferSchema",True).json("/mnt/formula1krdl/raw/drivers.json").filter("driverId BETWEEN 1 AND 5 or driverId BETWEEN 16 AND 20").select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.drivers_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC using DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge dm
# MAGIC USING drivers_day1 dd1
# MAGIC ON dm.driverId = dd1.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     dm.dob=dd1.dob,
# MAGIC     dm.forename=dd1.forename,
# MAGIC     dm.surname=dd1.surname,
# MAGIC     dm.updatedDate=current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge dm
# MAGIC USING drivers_day2 dd2
# MAGIC ON dm.driverId = dd2.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     dm.dob=dd2.dob,
# MAGIC     dm.forename=dd2.forename,
# MAGIC     dm.surname=dd2.surname,
# MAGIC     dm.updatedDate=current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.drivers_merge

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import current_timestamp

deltaTablePeople = DeltaTable.forPath(spark, '/mnt/formula1krdl/demo/drivers_merge')

deltaTablePeople.alias('dm') \
  .merge(
    drivers_day3_df.alias('dd3'),
    'dm.driverId = dd3.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
        "dm.dob":"dd3.dob",
        "dm.forename":"dd3.forename",
        "dm.surname":"dd3.surname",
        "dm.updatedDate":"current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
        "dm.driverId":"dd3.driverId",
        "dm.dob":"dd3.dob",
        "dm.forename":"dd3.forename",
        "dm.surname":"dd3.surname",
        "dm.createdDate":"current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versoning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccume

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 1

# COMMAND ----------

df=spark.read.format("delta").option("timestampAsOf",'2023-04-09T16:02:33.000+0000').load('/mnt/formula1krdl/demo/drivers_merge')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled=false

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2023-04-09T16:02:33.000+0000';

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE from f1_demo.drivers_merge WHERE driverId=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.drivers_merge VERSION AS OF 4;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 4 src
# MAGIC   ON(tgt.driverId=src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.drivers_txn(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC using DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId=1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId=2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn
# MAGIC WHERE driverId=2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %md
# MAGIC Convert from Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.drivers_convert_to_delta(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC using PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df=spark.table('f1_demo.drivers_convert_to_delta')

# COMMAND ----------

df.write.format('parquet').save('/mnt/formula1krdl/demo/drivers_convert_to_delta_new')

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1krdl/demo/drivers_convert_to_delta_new`
