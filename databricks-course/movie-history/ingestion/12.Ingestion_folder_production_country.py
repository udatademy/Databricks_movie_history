# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion de la carpeta "production_country"

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
# MAGIC #### Paso 1 - Leer los archivos JSON usando "DataFrameReader" de Spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType

# COMMAND ----------

productions_countries_schema = StructType(fields=[
    StructField("movieId", IntegerType(), True),
    StructField("countryId", IntegerType(), True)
])

# COMMAND ----------

productions_countries_df = spark.read \
                    .schema(productions_countries_schema) \
                    .option("multiLine", True) \
                    .json(f"{bronze_folder_path}/{v_file_date}/production_country")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieId" renombrar a "movie_id"
# MAGIC 2. "countryId" renombrar a "country_id"
# MAGIC 3. Agregar las columnas "ingestion_date" y "environment"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

productions_countries_final_df = productions_countries_df \
                            .withColumnsRenamed({"movieId": "movie_id",
                                                 "countryId": "country_id"})

# COMMAND ----------

productions_countries_final_df = add_ingestion_date(productions_countries_final_df) \
                                .withColumn("environment", lit(v_environment)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Escribir la salida en formato "Parquet"

# COMMAND ----------

#overwrite_partition(productions_countries_final_df, "movie_silver", "productions_countries", "file_date")

# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.country_id = src.country_id AND tgt.file_date = src.file_date'
merge_delta_lake(productions_countries_final_df, "movie_silver", "productions_countries", silver_folder_path, merge_condition, "file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT file_date, COUNT(1) 
# MAGIC FROM movie_silver.productions_countries
# MAGIC GROUP BY file_date;

# COMMAND ----------

dbutils.notebook.exit("Success")
