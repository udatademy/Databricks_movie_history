# Databricks notebook source
v_result = dbutils.notebook.run("01.Ingestion_file_movie", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("02.Ingestion_file_language", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("03.Ingestion_file_genre", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("04.Ingestion_file_country", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("05.Ingestion_file_person", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("06.Ingestion_file_movie_genre", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("07.Ingestion_file_movie_cast", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("08.Ingestion_file_language_role", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("09.Ingestion_folder_production_company", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("10.Ingestion_folder_movie_company", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("11.Ingestion_folder_movie_language", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("12.Ingestion_folder_production_country", 0, {"p_environment": "developer", "p_file_date": "2024-12-30"})

# COMMAND ----------

v_result
