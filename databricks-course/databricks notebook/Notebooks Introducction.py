# Databricks notebook source
# MAGIC %md
# MAGIC ### Introducción a Notebooks
# MAGIC #### Interfáz Gráfica
# MAGIC #### Comandos Mágicos
# MAGIC - %python
# MAGIC - %sql
# MAGIC - %scala
# MAGIC - %r

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comando Mágico %python

# COMMAND ----------

message = 'Bienvenido al Curso Master en Azure Databricks & Spark para Data Engineers'

# COMMAND ----------

print(message)

# COMMAND ----------

message = 'hello world'
display(message)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comando Mágico %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hola"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comando Mágico %scala

# COMMAND ----------

# MAGIC %scala
# MAGIC var msg = "Hola Mundo"
# MAGIC print(msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comando Mágico %r

# COMMAND ----------

# MAGIC %r
# MAGIC print("Hola Mundo")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comando Mágico %fs

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comando Mágico %sh

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comando Mágico %lsmagic

# COMMAND ----------

lsmagic

# COMMAND ----------

# MAGIC %env?
