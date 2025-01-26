# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion del archivo "country.json"

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
# MAGIC #### Paso 1 - Leer el archivo JSON usando "DataFrameReader" de Spark

# COMMAND ----------

countries_schema = "countryId INT, countryIsoCode STRING, countryName STRING"

# COMMAND ----------

countries_df = spark.read \
                .schema(countries_schema) \
                .json(f"{bronze_folder_path}/{v_file_date}/country.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Eliminar columnas no deseadas del DataFrame

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

countries_dropped_df = countries_df.drop(col("countryIsoCode"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Cambiar el nombre de las columnas y añadir "ingestion_date" y "environment"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

countries_final_df = countries_dropped_df \
                    .withColumnRenamed("countryId", "country_id") \
                    .withColumnRenamed("countryName", "country_name")

# COMMAND ----------

countries_final_df = add_ingestion_date(countries_final_df) \
                    .withColumn("environment", lit(v_environment)) \
                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un formato "Parquet"

# COMMAND ----------

countries_final_df.write.mode("overwrite").format("delta").saveAsTable("movie_silver.countries")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_silver.countries;

# COMMAND ----------

dbutils.notebook.exit("Success")
