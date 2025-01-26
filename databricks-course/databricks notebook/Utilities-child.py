# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_msg", "", "Parámetro de Entrada")

# COMMAND ----------

v_msg = dbutils.widgets.get("p_msg")

# COMMAND ----------

print("Soy el hijo del notebook: Utilities-introduction. " + v_msg)

# COMMAND ----------

dbutils.notebook.exit("Ejecución con exitos.")
