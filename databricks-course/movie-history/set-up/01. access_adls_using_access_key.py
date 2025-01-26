# Databricks notebook source
# MAGIC %md
# MAGIC ### Acceder a Azure Data Lake Storage mediante Access Key
# MAGIC 1. Establecer la configuración de spark "fs.azure.account.key"
# MAGIC 2. Listar archivos del contenedor "demo"
# MAGIC 3. Leer datos del archivo "movie.csv"

# COMMAND ----------

movie_access_key = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "movie-access-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.moviehistory.dfs.core.windows.net",
    movie_access_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistory.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistory.dfs.core.windows.net/movie.csv"))
