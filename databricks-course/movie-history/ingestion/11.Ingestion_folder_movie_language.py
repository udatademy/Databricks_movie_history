# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion de la carpeta "movie_language"

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

movies_languages_schema = StructType(fields=[
    StructField("movieId", IntegerType(), True),
    StructField("languageId", IntegerType(), True),
    StructField("languageRoleId", IntegerType(), True)
])

# COMMAND ----------

movies_languages_df = spark.read \
                    .schema(movies_languages_schema) \
                    .option("multiLine", True) \
                    .json(f"{bronze_folder_path}/{v_file_date}/movie_language")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieId" renombrar a "movie_id"
# MAGIC 2. "languageId" renombrar a "language_id"
# MAGIC 3. Agregar las columnas "ingestion_date" y "environment"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

movies_languages_with_columns_df = movies_languages_df \
                            .withColumnsRenamed({"movieId": "movie_id",
                                                 "languageId": "language_id"})

# COMMAND ----------

movies_languages_with_columns_df = add_ingestion_date(movies_languages_with_columns_df) \
                                    .withColumn("environment", lit(v_environment)) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Eliminar la columna "languageRoleId"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

movies_languages_final_df = movies_languages_with_columns_df.drop(col("languageRoleId"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en formato "Parquet"

# COMMAND ----------

#overwrite_partition(movies_languages_final_df, "movie_silver", "movies_languages", "file_date")

# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.language_id = src.language_id AND tgt.file_date = src.file_date'
merge_delta_lake(movies_languages_final_df, "movie_silver", "movies_languages", silver_folder_path, merge_condition, "file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT file_date, COUNT(1) 
# MAGIC FROM movie_silver.movies_languages
# MAGIC GROUP BY file_date;

# COMMAND ----------

dbutils.notebook.exit("Success")
