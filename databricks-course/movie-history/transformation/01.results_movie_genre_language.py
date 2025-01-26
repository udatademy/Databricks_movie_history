# Databricks notebook source
# MAGIC %md
# MAGIC ### Leer todos los datos que son requeridos

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-30")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

movies_df = spark.read.format("delta").load(f"{silver_folder_path}/movies") \
                        .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

languages_df = spark.read.format("delta").load(f"{silver_folder_path}/languages")

# COMMAND ----------

movies_languages_df = spark.read.format("delta").load(f"{silver_folder_path}/movies_languages")  \
                                .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

genres_df = spark.read.format("delta").load(f"{silver_folder_path}/genres")

# COMMAND ----------

movies_genres_df = spark.read.format("delta").load(f"{silver_folder_path}/movies_genres")  \
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "languages" y "movies_languages"

# COMMAND ----------

languages_mov_lan_df = languages_df.join(movies_languages_df,
                                         languages_df.language_id == movies_languages_df.language_id,
                                         "inner") \
                        .select(languages_df.language_name, languages_df.language_id,
                                 movies_languages_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "genres" y "movies_genres"

# COMMAND ----------

genres_mov_gen_df = genres_df.join(movies_genres_df,
                                   genres_df.genre_id == movies_genres_df.genre_id,
                                   "inner") \
                    .select(genres_df.genre_name, genres_df.genre_id, 
                            movies_genres_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "movies_df", "languages_mov_lan_df" y "genres_mov_lan_df"

# COMMAND ----------

# MAGIC %md
# MAGIC - Filtrar las películas donde su fecha de lanzamiento sea mayor o igual a 2000

# COMMAND ----------

movie_filter_df = movies_df.filter("year_release_date >= 2000")

# COMMAND ----------

results_movies_genres_languages_df = movie_filter_df.join(languages_mov_lan_df,
                                    movie_filter_df.movie_id == languages_mov_lan_df.movie_id,
                                    "inner") \
                                                    .join(genres_mov_gen_df,
                                    movie_filter_df.movie_id == genres_mov_gen_df.movie_id,
                                    "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC - Agregar la columna "created_date"

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_df = results_movies_genres_languages_df \
            .select(movie_filter_df.movie_id, "language_id", "genre_id", "title", "duration_time", "release_date", "vote_average", "language_name", "genre_name") \
            .withColumn("created_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC - Ordenar por la columna "release_date" de manera descendente

# COMMAND ----------

results_order_by_df = results_df.orderBy(results_df.release_date.desc())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Escribir datos en el DataLake en formato "Delta"

# COMMAND ----------

#overwrite_partition(results_order_by_df, "movie_gold", "results_movie_genre_language", "created_date")

# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.language_id = src.language_id AND tgt.genre_id = src.genre_id AND tgt.created_date = src.created_date'
merge_delta_lake(results_order_by_df, "movie_gold", "results_movie_genre_language", gold_folder_path, merge_condition, "created_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_gold.results_movie_genre_language
