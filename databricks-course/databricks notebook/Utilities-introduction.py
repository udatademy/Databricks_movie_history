# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("cp")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

for folder_name in dbutils.fs.ls("/databricks-datasets"):
    print(folder_name.path)

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run("./Utilities-child", 15, {"p_msg": "Ejecución desde el notebook padre."})
