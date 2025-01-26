# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion del archivo "language.csv"

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

languages_schema = StructType(fields=[
    StructField("languageId", IntegerType(), False),
    StructField("languageCode", StringType(), True),
    StructField("languageName", StringType(), True)
])

# COMMAND ----------

languages_df = spark.read \
                .option("header", True) \
                .schema(languages_schema) \
                .csv(f"{bronze_folder_path}/{v_file_date}/language.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 -Seleccionar sólo las columnas "requeridas"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

languages_selected_df = languages_df.select(col("languageId"), col("languageName"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 -Cambiar el nombre de las columnas según lo "requerido"

# COMMAND ----------

languages_renamed_df = languages_selected_df.withColumnsRenamed({"languageId": "language_id", "languageName": "language_name"})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 -Agregar las columnas "ingestion_date" y "enviroment" al DataFrame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

langagues_final_df = add_ingestion_date(languages_renamed_df) \
                    .withColumn("environment", lit(v_environment)) \
                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 5 - Escribir datos en el DataLake en formato "Parquet"

# COMMAND ----------

langagues_final_df.write.mode("overwrite").format("delta").saveAsTable("movie_silver.languages")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_silver.languages

# COMMAND ----------

dbutils.notebook.exit("Success")
