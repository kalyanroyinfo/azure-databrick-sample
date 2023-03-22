// Databricks notebook source
//Data Cleaning Process - Account Data

// COMMAND ----------

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,FloatType,DateType}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.{sha2,concat,col,regexp_replace,when,lit}

// COMMAND ----------



// COMMAND ----------

val account_schema = StructType(List(StructField("acc_id", StringType, false),
                                     StructField("mem_id", StringType, false),
                                     StructField("loan_id", StringType, false),
                                     StructField("grade", StringType, true),
                                     StructField("sub_grade",StringType, true),
                                     StructField("emp_title",StringType, true),
                                     StructField("emp_length",StringType, true),
                                     StructField("home_ownership",StringType, true),
                                     StructField("annual_inc",FloatType, true),
                                     StructField("verification_status",StringType, true),
                                     StructField("tot_hi_cred_lim",FloatType, true),
                                     StructField("application_type",StringType, true),
                                     StructField("annual_inc_joint",FloatType, true),
                                     StructField("verification_status_joint",StringType, true)
                                    
))

// COMMAND ----------

//create a reader dataframe for customer's data
val account_df = spark.read.option("header", true).schema(account_schema).csv("/mnt/extdbstore/raw-data-test/lending_loan/account_details.csv")

// COMMAND ----------

display(account_df)

// COMMAND ----------

account_df.createOrReplaceTempView("temp")
val unique_values=spark.sql("select distinct emp_length from temp")
display(unique_values)

// COMMAND ----------

val replace_value_na=account_df.withColumn("emp_length", when(col("emp_length")=== lit("n/a"),lit("null")).otherwise(col("emp_length")))
//display(replace_value_na)
replace_value_na.createOrReplaceTempView("temp")
val unique_values1=spark.sql("select distinct emp_length from temp")
display(unique_values1)


// COMMAND ----------

val replace_value_1year=replace_value_na.withColumn("emp_length", when(col("emp_length")=== lit("< 1 year"),lit("1")).otherwise(col("emp_length")))
val replace_value_10year=replace_value_1year.withColumn("emp_length", when(col("emp_length")=== lit("10+ years"),lit("10")).otherwise(col("emp_length")))

// COMMAND ----------

// Define the string to remove
val string_to_remove = "years"
 
// Use the regexp_replace function to remove the string from the column
val clean_emp_length_df = replace_value_10year.withColumn("emp_length", regexp_replace(replace_value_10year.col("emp_length"), string_to_remove, ""))
 
// Display the resulting dataframe
display(clean_emp_length_df)

// COMMAND ----------

// Define the string to remove
val string_to_remove = "year"
 
// Use the regexp_replace function to remove the string from the column
val final_clean_emp_length_df = clean_emp_length_df.withColumn("emp_length", regexp_replace(clean_emp_length_df.col("emp_length"), string_to_remove, ""))
 
// Display the resulting dataframe
display(final_clean_emp_length_df)

// COMMAND ----------

final_clean_emp_length_df.createOrReplaceTempView("temp")
val unique_values=spark.sql("select distinct emp_length from temp")
display(unique_values)

// COMMAND ----------

//Null remove from data
val account_final_null_remove_df=final_clean_emp_length_df.na.fill("")

// COMMAND ----------

account_final_null_remove_df.createOrReplaceTempView("temp")
val display_df=spark.sql("select * from temp where tot_hi_cred_lim is null ")
display(display_df)

// COMMAND ----------

val account_df_ingestDate=account_final_null_remove_df.withColumn("ingest_date", current_timestamp())
display(account_df_ingestDate)

// COMMAND ----------

val account_df_key=account_df_ingestDate.withColumn("account_key", sha2(concat(col("acc_id"),col("mem_id"),col("loan_id")), 256))
display(account_df_key)

// COMMAND ----------

val account_df_rename=account_df_key.withColumnRenamed("acc_id","account_id").withColumnRenamed("mem_id","member_id").withColumnRenamed("emp_title","employee_designation").withColumnRenamed("emp_length","employee_experience").withColumnRenamed("annual_inc","annual_income").withColumnRenamed("tot_hi_cred_lim","total_high_credit_limit").withColumnRenamed("annual_inc_joint","annual_income_joint")

display(account_df_rename)

// COMMAND ----------

account_df_rename.createOrReplaceTempView("temp_table")
val final_df=spark.sql("select account_key,ingest_date,account_id,member_id,loan_id,grade,sub_grade,employee_designation,employee_experience,home_ownership,annual_income,verification_status,total_high_credit_limit,application_type,annual_income_joint,verification_status_joint from temp_table")
display(final_df)

// COMMAND ----------

final_df.write.option("header",true).mode(SaveMode.Append).parquet("mnt/extdbstore/processed-data/lending_loan/account_details")

// COMMAND ----------

display(spark.read.parquet("/mnt/extdbstore/processed-data/lending_loan/account_details"))

// COMMAND ----------

dbutils.notebook.exit("executed account job")
