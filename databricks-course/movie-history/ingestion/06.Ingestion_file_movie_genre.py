# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion del archivo "movie_genre.json"

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
# MAGIC #### Paso 1 - Leer el archivo JSON usando "DataFrameReader" de Spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

movies_genres_schema = StructType(fields=[
    StructField("movieId", IntegerType(), True),
    StructField("genreId", IntegerType(), True)
])

# COMMAND ----------

movies_genres_df = spark.read \
                    .schema(movies_genres_schema) \
                    .json(f"{bronze_folder_path}/{v_file_date}/movie_genre.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "movieId" renombrar a "movie_id"
# MAGIC 2. "genreId" renombrar a "genre_id"
# MAGIC 3. Agregar las columnas "ingestion_date" y "environment"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

movies_genres_final_df = movies_genres_df \
                        .withColumnRenamed("movieId", "movie_id") \
                        .withColumnRenamed("genreId", "genre_id")

# COMMAND ----------

movies_genres_final_df = add_ingestion_date(movies_genres_final_df) \
                        .withColumn("environment", lit(v_environment)) \
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Escribir la salida en formato "Parquet"

# COMMAND ----------

#overwrite_partition(movies_genres_final_df, "movie_silver", "movies_genres", "file_date")

# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.genre_id = src.genre_id AND tgt.file_date = src.file_date'
merge_delta_lake(movies_genres_final_df, "movie_silver", "movies_genres", silver_folder_path, merge_condition, "file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT file_date, COUNT(1) 
# MAGIC FROM movie_silver.movies_genres
# MAGIC GROUP BY file_date;

# COMMAND ----------

dbutils.notebook.exit("Success")
