# Databricks notebook source
# MAGIC %md
# MAGIC ### Acceso al DataFrame mediante SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### Local Temp View
# MAGIC   1. Crear vista(view) temporal en la Base de Datos
# MAGIC   2. Acceder a la vista(view) desde la celda "SQL"
# MAGIC   3. Acceder a la vista(view) desde la celda "Python"
# MAGIC   4. Acceder a la vista desde otro Notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

resuls_movie_genre_language_df = spark.read.parquet(f"{gold_folder_path}/results_movie_genre_language")

# COMMAND ----------

resuls_movie_genre_language_df.createOrReplaceTempView("v_movie_genre_language")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM v_movie_genre_language
# MAGIC WHERE vote_average > 7.5

# COMMAND ----------

p_vote_average = 7.5

# COMMAND ----------

results_movie_genre_language_2 = spark.sql(f"SELECT * FROM v_movie_genre_language WHERE vote_average > {p_vote_average}")

# COMMAND ----------

display(results_movie_genre_language_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temp View

# COMMAND ----------

# MAGIC %md
# MAGIC #### Objetivos
# MAGIC   1. Crear una vista(view) temporal global del DataFrame
# MAGIC   2. Acceder a la vista(view) desde la celda "SQL"
# MAGIC   3. Acceder a la vista(view) desde la celda "Python"
# MAGIC   4. Acceder a la vista desde otro "Notebook"

# COMMAND ----------

resuls_movie_genre_language_df.createOrReplaceGlobalTempView("gv_movie_genre_language")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_movie_genre_language

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_movie_genre_language").display()
