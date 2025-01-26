# Databricks notebook source
# MAGIC %md
# MAGIC ### Acceder a Azure Data Lake Storage mediante Token Sas
# MAGIC 1. Establecer la configuración de spark "SAS Token"
# MAGIC 2. Listar archivos del contenedor "demo"
# MAGIC 3. Leer datos del archivo "movie.csv"

# COMMAND ----------

movie_sas_token = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "movie-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.moviehistory.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.moviehistory.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.moviehistory.dfs.core.windows.net", 
               movie_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistory.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistory.dfs.core.windows.net/movie.csv"))
