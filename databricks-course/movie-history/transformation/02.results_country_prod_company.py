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

production_country_df = spark.read.format("delta").load(f"{silver_folder_path}/productions_countries")  \
                                    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

production_company_df  = spark.read.format("delta").load(f"{silver_folder_path}/productions_companies")  \
                                    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

movie_company_df = spark.read.format("delta").load(f"{silver_folder_path}/movies_companies")  \
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "country" y "production_country"

# COMMAND ----------

country_prod_coun_df = country_df.join(production_country_df,
                                         country_df.country_id == production_country_df.country_id,
                                         "inner") \
                        .select(country_df.country_name, country_df.country_id, 
                                production_country_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "production_company" y "movie_company"

# COMMAND ----------

prod_comp_mov_comp_df = production_company_df.join(movie_company_df,
                                   production_company_df.company_id == movie_company_df.company_id,
                                   "inner") \
                    .select(production_company_df.company_name, production_company_df.company_id, 
                            movie_company_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "movies_df", "country_prod_coun_df" y "prod_comp_mov_comp_df"

# COMMAND ----------

# MAGIC %md
# MAGIC - Filtrar las películas donde su fecha de lanzamiento sea mayor o igual a 2010

# COMMAND ----------

movie_filter_df = movies_df.filter("year_release_date >= 2010")

# COMMAND ----------

results_movies_country_prod_company_df = movie_filter_df.join(country_prod_coun_df,
                                    movie_filter_df.movie_id == country_prod_coun_df.movie_id,
                                    "inner") \
                                                    .join(prod_comp_mov_comp_df,
                                    movie_filter_df.movie_id == prod_comp_mov_comp_df.movie_id,
                                    "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC - Agregar la columna "created_date"

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_df = results_movies_country_prod_company_df \
            .select(movie_filter_df.movie_id, "country_id", "company_id","title", "budget", "revenue", "duration_time", "release_date", "country_name", "company_name") \
            .withColumn("created_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC - Ordenar por la columna "title" de manera ascendente

# COMMAND ----------

results_order_by_df = results_df.orderBy(results_df.title.asc())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Escribir datos en el DataLake en formato "delta"

# COMMAND ----------

#overwrite_partition(results_order_by_df, "movie_gold", "results_country_prod_company", "created_date")

# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.country_id = src.country_id AND tgt.company_id = src.company_id AND tgt.created_date = src.created_date'
merge_delta_lake(results_order_by_df, "movie_gold", "results_country_prod_company", gold_folder_path, merge_condition, "created_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_gold.results_country_prod_company;
