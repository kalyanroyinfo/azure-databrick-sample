# Databricks notebook source
v_results=dbutils.notebook.run("1.ingest_circuits_files",0,{"p_data_source":"ergest_api"})
v_results

# COMMAND ----------

v_results=dbutils.notebook.run("2.ingest_races_files",0,{"p_data_source":"ergest_api"})
v_results

# COMMAND ----------

v_results=dbutils.notebook.run("3.ingest_constructor_files",0,{"p_data_source":"ergest_api"})
v_results

# COMMAND ----------

v_results=dbutils.notebook.run("4.ingest_drivers_files",0,{"p_data_source":"ergest_api"})
v_results

# COMMAND ----------

v_results=dbutils.notebook.run("5.ingest_results_files",0,{"p_data_source":"ergest_api"})
v_results

# COMMAND ----------

v_results=dbutils.notebook.run("6.ingest_pitstops_files",0,{"p_data_source":"ergest_api"})
v_results

# COMMAND ----------

v_results=dbutils.notebook.run("7.ingest_laptimes_files",0,{"p_data_source":"ergest_api"})
v_results

# COMMAND ----------

v_results=dbutils.notebook.run("8.ingest_qualifying_files",0,{"p_data_source":"ergest_api"})
v_results
