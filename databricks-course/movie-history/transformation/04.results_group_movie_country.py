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

country_df = spark.read.format("delta").load(f"{silver_folder_path}/countries")

# COMMAND ----------

production_country_df = spark.read.format("delta").load(f"{silver_folder_path}/productions_countries") \
                                .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join "country" y "production_country"

# COMMAND ----------

country_prod_coun_df = country_df.join(production_country_df,
                                       country_df.country_id == production_country_df.country_id,
                                       "inner") \
                                .select(country_df.country_name,
                                        production_country_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join "movies_df" y "country_prod_coun_df"

# COMMAND ----------

# MAGIC %md
# MAGIC   - Filtrar las películas donde su fecha de lanzamiento sea mayor o igual a 2015

# COMMAND ----------

movies_filter_df = movies_df.filter("year_release_date >= 2015")

# COMMAND ----------

results_movies_country_prod_coun_df = movies_filter_df.join(country_prod_coun_df,
                                            movies_filter_df.movie_id == country_prod_coun_df.movie_id,
                                            "inner")

# COMMAND ----------

results_df = results_movies_country_prod_coun_df.select("year_release_date", "country_name", 
                                                        "budget", "revenue")

# COMMAND ----------

from pyspark.sql.functions import sum

results_group_by_df = results_df \
                    .groupBy("year_release_date", "country_name") \
                    .agg(
                        sum("budget").alias("total_budget"),
                        sum("revenue").alias("total_revenue")
                    )

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, dense_rank

results_dense_rank_df = Window.partitionBy("year_release_date").orderBy(
                                                                desc("total_budget"), 
                                                                desc("total_revenue"))
final_df = results_group_by_df.withColumn("dense_rank", dense_rank().over(results_dense_rank_df)) \
                                .withColumn("created_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escribir datos en el DataLake en formato "delta"

# COMMAND ----------

#overwrite_partition(final_df, "movie_gold", "results_group_movie_country", "created_date")

# COMMAND ----------

merge_condition = 'tgt.year_release_date = src.year_release_date AND tgt.country_name = src.country_name AND tgt.created_date = src.created_date'
merge_delta_lake(final_df, "movie_gold", "results_group_movie_country", gold_folder_path, merge_condition, "created_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_gold.results_group_movie_country
