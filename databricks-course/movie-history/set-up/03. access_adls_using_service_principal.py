# Databricks notebook source
# MAGIC %md
# MAGIC ### Acceder a Azure Data Lake Storage mediante Service Principal
# MAGIC 1. "Registrar la Aplicación" en Azure Entra ID / Service Principal
# MAGIC 2. Generar un secreto(Contraseña) para la aplicación
# MAGIC 3. Configurar Spark con APP / Client Id, Directory / Tenand Id & Secret
# MAGIC 4. Asignar el Role "Storage Blob Data Contributor" al Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-id")
tenant_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "tenant-id")
client_secret = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.moviehistory.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.moviehistory.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.moviehistory.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.moviehistory.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.moviehistory.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistory.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistory.dfs.core.windows.net/movie.csv"))
