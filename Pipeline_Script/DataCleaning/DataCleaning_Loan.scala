// Databricks notebook source
//Data Cleaning Process - Loan Data

// COMMAND ----------

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,FloatType,DateType}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.{sha2,concat,col,regexp_replace}

// COMMAND ----------

val loan_schema = StructType(List(StructField("loan_id", StringType, true),
                                     StructField("mem_id", StringType, true),
                                     StructField("acc_id", StringType, true),
                                     StructField("loan_amt", DoubleType, true),
                                     StructField("fnd_amt", DoubleType, true),
                                     StructField("term", StringType, true),
                                     StructField("interest", StringType, true),
                                     StructField("installment", FloatType, true),
                                     StructField("issue_date", DateType, true),
                                     StructField("loan_status", StringType, true),
                                     StructField("purpose", StringType, true),
                                     StructField("title", StringType, true),
                                     StructField("disbursement_method", StringType, true)
                                    
))

// COMMAND ----------

//create a reader dataframe for customer's data
val loan_df = spark.read.option("header", true).schema(loan_schema).csv("/mnt/extdbstore/raw-data-test/lending_loan/loan_details.csv")

// COMMAND ----------

display(loan_df)

// COMMAND ----------

loan_df.createOrReplaceTempView("loan_table")
val loan_sql=spark.sql("select * from loan_table")
display(loan_sql)

// COMMAND ----------

val loan_sql=spark.sql("select * from loan_table where term=36 or interest > 5.0")
display(loan_sql)

// COMMAND ----------

// Define the string to remove
val string_to_remove = "months"
 
// Use the regexp_replace function to remove the string from the column
val clean_term_df = loan_df.withColumn("term", regexp_replace(loan_df.col("term"), string_to_remove, ""))
 
// Display the resulting dataframe
display(clean_term_df)

// COMMAND ----------

// Define the string to remove
val string_to_remove = "%"
 
// Use the regexp_replace function to remove the string from the column
val clean_interest_df = clean_term_df.withColumn("interest", regexp_replace(loan_df.col("interest"), string_to_remove, ""))
 
// Display the resulting dataframe
display(clean_interest_df)

// COMMAND ----------

clean_interest_df.createOrReplaceTempView("loan_clean_df")
val clean_loan_sql=spark.sql("select * from loan_clean_df where term=36 or interest > 10.0")
display(clean_loan_sql)

// COMMAND ----------

val loan_df_ingestDate=clean_interest_df.withColumn("ingest_date", current_timestamp())
display(loan_df_ingestDate)

// COMMAND ----------

val loan_df_rename=loan_df_ingestDate.withColumnRenamed("mem_id","member_id").withColumnRenamed("acc_id","account_id").withColumnRenamed("loan_amt","loan_amount").withColumnRenamed("fnd_amt","funded_amount")
 
val loan_df_key=loan_df_rename.withColumn("loan_key", sha2(concat(col("loan_id"),col("member_id"),col("loan_amount")), 256))
display(loan_df_key)

// COMMAND ----------

loan_df_key.createOrReplaceTempView("df_null")
val null_df=spark.sql("select * from df_null where interest='null' ")
display(null_df)

// COMMAND ----------


val final_df=loan_df_key.na.fill("None")
display(final_df)

// COMMAND ----------

final_df.createOrReplaceTempView("df_null")
val null_removed=spark.sql("select * from df_null where interest='null' ")
display(null_removed)

// COMMAND ----------

final_df.createOrReplaceTempView("temp_table")
val display_df=spark.sql("select loan_key, ingest_date,loan_id,member_id,account_id,loan_amount,funded_amount,term,interest,installment,issue_date,loan_status,purpose,title,disbursement_method from temp_table")
display(display_df)

// COMMAND ----------

display_df.write.option("header",true).mode(SaveMode.Append).parquet("mnt/extdbstore/processed-data/lending_loan/loan_details")

// COMMAND ----------

display(spark.read.parquet("/mnt/extdbstore/processed-data/lending_loan/loan_details"))

// COMMAND ----------

dbutils.notebook.exit("executed loan job")
