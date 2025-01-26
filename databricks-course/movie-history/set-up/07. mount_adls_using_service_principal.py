# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount(Montar) Azure Data Lake mediante service Principal
# MAGIC 1. Obtener el valor client_id, tenant_id y client_secret del key Vault
# MAGIC 2. Configurar Spark con APP/Client Id, Directory/Tenant Id & Secret
# MAGIC 3. Utilizar el método "mount" de "utility" para montar el almacenamiento
# MAGIC 4. Explorar otras utilidades del sistema de archivos relacionados con el montaje(list all mounts, unmounts)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-id")
tenant_id = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "tenant-id")
client_secret = dbutils.secrets.get(scope = "movie-history-secret-scope", key = "client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@moviehistory.dfs.core.windows.net/",
  mount_point = "/mnt/moviehistory/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/moviehistory/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/moviehistory/demo/movie.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/moviehistory/demo")
