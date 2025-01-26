# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion del archivo "language_role.json"

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

languages_roles_schema = StructType(fields=[
    StructField("roleId", IntegerType(), True),
    StructField("languageRole", StringType(), True)
])

# COMMAND ----------

languages_roles_df = spark.read \
                    .schema(languages_roles_schema) \
                    .option("multiLine", True) \
                    .json(f"{bronze_folder_path}/{v_file_date}/language_role.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# MAGIC 1. "roleId" renombrar a "role_id"
# MAGIC 2. "languageRole" renombrar a "language_role"
# MAGIC 3. Agregar las columnas "ingestion_date" y "environment"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

languages_roles_final_df = languages_roles_df \
                            .withColumnsRenamed({"roleId": "role_id",
                                                 "languageRole": "language_role"})

# COMMAND ----------

languages_roles_final_df = add_ingestion_date(languages_roles_final_df) \
                            .withColumn("environment", lit(v_environment)) \
                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Escribir la salida en formato "Parquet"

# COMMAND ----------

languages_roles_final_df.write.mode("overwrite").format("delta").saveAsTable("movie_silver.languages_roles")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_silver.languages_roles;

# COMMAND ----------

dbutils.notebook.exit("Success")
