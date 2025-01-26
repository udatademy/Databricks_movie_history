# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion del archivo "genre.csv"

# COMMAND ----------

dbutils.widgets.text("p_environment", "")
v_environment = dbutils.widgets.get("p_environment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-16")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 1 - Leer el archivo CSV usando "DataFrameReader" de Spark

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

genres_schema = StructType(fields=[
    StructField("genreId", IntegerType(), False),
    StructField("genreName", StringType(), True)
])

# COMMAND ----------

genres_df = spark.read \
            .option("header", True) \
            .schema(genres_schema) \
            .csv(f"{bronze_folder_path}/{v_file_date}/genre.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Cambiar el nombre de las columnas según lo "requerido"

# COMMAND ----------

genres_renamed_df = genres_df.withColumnsRenamed({"genreId": "genre_id", "genreName": "genre_name"})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Agregar las columnas "ingestion_date" y "enviroment" al DataFrame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

genres_final_df = add_ingestion_date(genres_renamed_df) \
                    .withColumn("environment", lit(v_environment)) \
                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir datos en el DataLake en formato "Parquet"

# COMMAND ----------

genres_final_df.write.mode("overwrite").format("delta").saveAsTable("movie_silver.genres")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_silver.genres

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_silver.genres;

# COMMAND ----------

dbutils.notebook.exit("Success")
