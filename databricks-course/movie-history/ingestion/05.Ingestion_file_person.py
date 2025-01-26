# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion del archivo "person.json"

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

persons_schema = StructType(fields=[
    StructField("personId", IntegerType(), False),
    StructField("personName", name_schema)
])

# COMMAND ----------

persons_df = spark.read \
            .schema(persons_schema) \
            .json(f"{bronze_folder_path}/{v_file_date}/person.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "personId" renombrar a "person_id"
# MAGIC 2. Agregar las columnas "ingestion_date" y "environment"
# MAGIC 3. Agregar la columna "name" a partir de la concatenación de "forename" y "surname"

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

persons_with_columns_df = persons_df \
                        .withColumnRenamed("personId", "person_id") \
                        .withColumn("name", 
                                    concat(
                                        col("personName.forename"), 
                                        lit(" "), 
                                        col("personName.surname")
                                        )
                                    )

# COMMAND ----------

persons_with_columns_df = add_ingestion_date(persons_with_columns_df) \
                            .withColumn("environment", lit(v_environment)) \
                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Eliminar las columnas no "requeridas"

# COMMAND ----------

persons_final_df = persons_with_columns_df.drop(col("personName"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un formato "Parquet"

# COMMAND ----------

#overwrite_partition(persons_final_df, "movie_silver", "persons", "file_date")

# COMMAND ----------

merge_condition = 'tgt.person_id = src.person_id AND tgt.file_date = src.file_date'
merge_delta_lake(persons_final_df, "movie_silver", "persons", silver_folder_path, merge_condition, "file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT file_date, COUNT(1) 
# MAGIC FROM movie_silver.persons
# MAGIC GROUP BY file_date;

# COMMAND ----------

dbutils.notebook.exit("Success")
