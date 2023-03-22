// Databricks notebook source
//python
//Data Cleaning Process - Customers Data

// COMMAND ----------

/*
Step 01:
Study and analyze the data
Come up with scope for cleaning the data
Bring out the cleaning techniques to be applied
Include the new columns to be added
*/

// COMMAND ----------

/*
Step 02:
Infer the schema of the tables
Read the data from the files
Query the data
Data cleaning techniques
New column additions
Write the data to data lake
*/

// COMMAND ----------

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

// COMMAND ----------

//Infer the schema of customer's data
val customer_schema = StructType(List(StructField("cust_id", StringType, true),
                                     StructField("mem_id", StringType, true),
                                     StructField("fst_name", StringType, false),
                                     StructField("lst_name", StringType, false),
                                     StructField("prm_status", StringType, false),
                                     StructField("age", IntegerType, false),
                                     StructField("state", StringType, false),
                                     StructField("country", StringType, false)
                                    
))

// COMMAND ----------

//create a reader dataframe for customer's data
val customer_df = spark.read.option("header", true).schema(customer_schema).csv("/mnt/extdbstore/raw-data-test/lending_loan/loan_customer_data.csv")

// COMMAND ----------

customer_df.show()

// COMMAND ----------

//Rename the columns to a better understandable way
val customer_df_change=customer_df.withColumnRenamed("cust_id","customer_id")
.withColumnRenamed("mem_id","member_id")
.withColumnRenamed("fst_name","first_name")
.withColumnRenamed("lst_name","last_name")
.withColumnRenamed("prm_status","premium_status")

// COMMAND ----------

 import org.apache.spark.sql.functions.current_timestamp
val customer_df_ingestDate=customer_df_change.withColumn("ingest_date", current_timestamp())
customer_df_ingestDate.show()

// COMMAND ----------

 import org.apache.spark.sql.functions.{sha2,concat,col}
val customer_df_final=customer_df_ingestDate.withColumn("customer_key", sha2(concat(col("member_id"),col("age"),col("state")), 256))
display(customer_df_final)

// COMMAND ----------

customer_df_final.createOrReplaceTempView("temp_table")
val display_df=spark.sql("select customer_key,ingest_date,customer_id,member_id,first_name,last_name,premium_status,age,state,country from temp_table")
display(display_df)

// COMMAND ----------

display_df.write.option("header",true).mode(SaveMode.Append).parquet("/mnt/extdbstore/processed-data/lending_loan/customer_details")

// COMMAND ----------

dbutils.fs.ls("/mnt/extdbstore/processed-data/lending_loan/customer_details")

// COMMAND ----------

display(spark.read.parquet("/mnt/extdbstore/processed-data/lending_loan/customer_details"))

// COMMAND ----------

dbutils.notebook.exit("executed customer job")
