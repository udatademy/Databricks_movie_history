# Databricks notebook source
# MAGIC %md
# MAGIC ### Explorar las capacidades de la utilidad "dbutils.secrets"

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = "movie-history-secret-scope")

# COMMAND ----------

dbutils.secrets.get(scope = "movie-history-secret-scope", key = "movie-access-key")
