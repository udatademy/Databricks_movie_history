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

genres_df = spark.read.format("delta").load(f"{silver_folder_path}/genres")

# COMMAND ----------

movies_genres_df = spark.read.format("delta").load(f"{silver_folder_path}/movies_genres") \
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join "genres" y "movies_genres"

# COMMAND ----------

genres_mov_gen_df = genres_df.join(movies_genres_df,
                                   genres_df.genre_id == movies_genres_df.genre_id,
                                   "inner") \
                    .select(genres_df.genre_name,
                            movies_genres_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join "movies_df" y "genres_mov_gen_df"

# COMMAND ----------

# MAGIC %md
# MAGIC   - Filtrar las películas donde su fecha de lanzamiento sea mayor o igual a 2015

# COMMAND ----------

movies_filter_df = movies_df.filter("year_release_date >= 2015")

# COMMAND ----------

results_movies_genres_df = movies_filter_df.join(genres_mov_gen_df,
                                                 movies_filter_df.movie_id == genres_mov_gen_df.movie_id,
                                                 "inner")

# COMMAND ----------

results_df = results_movies_genres_df.select("year_release_date", "genre_name", "budget", "revenue")

# COMMAND ----------

from pyspark.sql.functions import sum

results_group_by_df = results_df \
                    .groupBy("year_release_date", "genre_name") \
                    .agg(
                        sum("budget").alias("total_budget"),
                        sum("revenue").alias("total_revenue")
                    )

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, dense_rank, asc

results_dense_rank_df = Window.partitionBy("year_release_date").orderBy(
                                                                desc("total_budget"), 
                                                                desc("total_revenue"))
final_df = results_group_by_df.withColumn("dense_rank", dense_rank().over(results_dense_rank_df)) \
                                .withColumn("created_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escribir datos en el DataLake en formato "delta"

# COMMAND ----------

#overwrite_partition(final_df, "movie_gold", "results_group_movie_genre", "created_date")

# COMMAND ----------

merge_condition = 'tgt.year_release_date = src.year_release_date AND tgt.genre_name = src.genre_name AND tgt.created_date = src.created_date'
merge_delta_lake(final_df, "movie_gold", "results_group_movie_genre", gold_folder_path, merge_condition, "created_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_gold.results_group_movie_genre
