# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion de la carpeta "production_company"

# COMMAND ----------

dbutils.widgets.text("p_environment", "")
v_environment = dbutils.widgets.get("p_environment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-30")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 1 - Leer los archivos CSV usando "DataFrameReader" de Spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

productions_companies_schema = StructType(fields=[
    StructField("companyId", IntegerType(), True),
    StructField("companyName", StringType(), True)
])

# COMMAND ----------

productions_companies_df = spark.read \
                    .schema(productions_companies_schema) \
                    .csv(f"{bronze_folder_path}/{v_file_date}/production_company")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "companyId" renombrar a "company_id"
# MAGIC 2. "companyName" renombrar a "company_name"
# MAGIC 3. Agregar las columnas "ingestion_date" y "environment"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

productions_companies_final_df = productions_companies_df \
                            .withColumnsRenamed({"companyId": "company_id",
                                                 "companyName": "company_name"})

# COMMAND ----------

productions_companies_final_df = add_ingestion_date(productions_companies_final_df) \
                                .withColumn("environment", lit(v_environment)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Escribir la salida en formato "Parquet"

# COMMAND ----------

#overwrite_partition(productions_companies_final_df, "movie_silver", "productions_companies", "file_date")

# COMMAND ----------

merge_condition = 'tgt.company_id = src.company_id AND tgt.file_date = src.file_date'
merge_delta_lake(productions_companies_final_df, "movie_silver", "productions_companies", silver_folder_path, merge_condition, "file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT file_date, COUNT(1) 
# MAGIC FROM movie_silver.productions_companies
# MAGIC GROUP BY file_date;

# COMMAND ----------

dbutils.notebook.exit("Success")
