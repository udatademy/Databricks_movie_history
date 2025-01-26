# Databricks notebook source
# MAGIC %md
# MAGIC ### Acceder a Azure Data Lake Storage mediante Ámbito de Cluster
# MAGIC #### Pasos a seguir:
# MAGIC 1. Establecer la configuración de spark "fs.azure.account.key" en el Cluster
# MAGIC 2. Listar archivos del contenedor "demo"
# MAGIC 3. Leer datos del archivo "movie.csv"

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistory.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistory.dfs.core.windows.net/movie.csv"))
