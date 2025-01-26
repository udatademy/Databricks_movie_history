# Databricks notebook source
# MAGIC %md
# MAGIC ### Explorar DBFS root
# MAGIC 1. Mostrar los directorios en el almacenamiento DBFS
# MAGIC 2. Mostrar el contenido de un directorio DBFS dentro del root
# MAGIC - Con DBFS
# MAGIC - Sin DBFS
# MAGIC 3. Mostrar el contenido del sistema de archivos local
# MAGIC 4. Interacturar con el Explorador de Archivos DBFS
# MAGIC 5. Cargar archivo en el FileStore

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls file:/

# COMMAND ----------

display(dbutils.fs.ls("/FileStore"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/
