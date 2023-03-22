// Databricks notebook source
// MAGIC %run "/Users/kalyanroyinfo@gmail.com/Pipeline_Script/Call_functions_variables"

// COMMAND ----------

val status_customers=dbutils.notebook.run(cleanedScript_folder_path+"DataCleaning_customers",0)

// COMMAND ----------

if (status_customers.equalsIgnoreCase("executed customer job"))
    print("customers job completed successfully")

// COMMAND ----------

val status_loan=dbutils.notebook.run(cleanedScript_folder_path+"DataCleaning_Loan",0)

// COMMAND ----------

if (status_loan.equalsIgnoreCase("executed loan job"))
    print("Loan job completed successfully")

// COMMAND ----------

val status_account=dbutils.notebook.run(cleanedScript_folder_path+"DataCleaning_Account",0)

// COMMAND ----------

if (status_account.equalsIgnoreCase("executed account job"))
    print("Account job completed successfully")
