# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark Join Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC #### INNER JOIN

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

movie_df = spark.read.parquet(f"{silver_folder_path}/movies").filter("year_release_date = 2007")

# COMMAND ----------

production_country_df = spark.read.parquet(f"{silver_folder_path}/productions_countries")

# COMMAND ----------

country_df = spark.read.parquet(f"{silver_folder_path}/countries")

# COMMAND ----------

display(movie_df)

# COMMAND ----------

display(production_country_df)

# COMMAND ----------

display(country_df)

# COMMAND ----------

movie_production_country_df = movie_df.join(production_country_df,
                            movie_df.movie_id == production_country_df.movie_id,
                            "inner") \
                            .select(movie_df.title, movie_df.budget,
                                    production_country_df.country_id)

# COMMAND ----------

movie_country_df = movie_production_country_df.join(country_df,
                                                    movie_production_country_df.country_id == country_df.country_id,
                                                    "inner") \
                    .select(movie_production_country_df.title, movie_production_country_df.budget,
                            country_df.country_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outer Join

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Left Outer Join

# COMMAND ----------

movie_production_country_df = movie_df.join(production_country_df,
                            movie_df.movie_id == production_country_df.movie_id,
                            "left") \
                            .select(movie_df.title, movie_df.budget,
                                    production_country_df.country_id)

# COMMAND ----------

movie_country_df = movie_production_country_df.join(country_df,
                                                    movie_production_country_df.country_id == country_df.country_id,
                                                    "left") \
                    .select(movie_production_country_df.title, movie_production_country_df.budget,
                            country_df.country_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Right Outer Join

# COMMAND ----------

movie_production_country_df = movie_df.join(production_country_df,
                            movie_df.movie_id == production_country_df.movie_id,
                            "right") \
                            .select(movie_df.title, movie_df.budget,
                                    production_country_df.country_id)

# COMMAND ----------

movie_country_df = movie_production_country_df.join(country_df,
                                                    movie_production_country_df.country_id == country_df.country_id,
                                                    "right") \
                    .select(movie_production_country_df.title, movie_production_country_df.budget,
                            country_df.country_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Full Outer Join

# COMMAND ----------

movie_production_country_df = movie_df.join(production_country_df,
                            movie_df.movie_id == production_country_df.movie_id,
                            "full") \
                            .select(movie_df.title, movie_df.budget,
                                    production_country_df.country_id)

# COMMAND ----------

movie_country_df = movie_production_country_df.join(country_df,
                                                    movie_production_country_df.country_id == country_df.country_id,
                                                    "full") \
                    .select(movie_production_country_df.title, movie_production_country_df.budget,
                            country_df.country_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Semi Join

# COMMAND ----------

movie_production_country_df = movie_df.join(production_country_df,
                            movie_df.movie_id == production_country_df.movie_id,
                            "left") \
                            .select(movie_df.title, movie_df.budget,
                                    production_country_df.country_id)

# COMMAND ----------

movie_country_df = movie_production_country_df.join(country_df,
                                                    movie_production_country_df.country_id == country_df.country_id,
                                                    "semi") \
                    .select(movie_production_country_df.title, movie_production_country_df.budget)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Anti Join

# COMMAND ----------

movie_production_country_df = movie_df.join(production_country_df,
                            movie_df.movie_id == production_country_df.movie_id,
                            "left") \
                            .select(movie_df.title, movie_df.budget,
                                    production_country_df.country_id)

# COMMAND ----------

movie_country_df = movie_production_country_df.join(country_df,
                                                    movie_production_country_df.country_id == country_df.country_id,
                                                    "anti") \
                    .select(movie_production_country_df.title, movie_production_country_df.budget)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cross Join

# COMMAND ----------

movie_country_df = movie_df.crossJoin(country_df)

# COMMAND ----------

display(movie_country_df)

# COMMAND ----------

display(movie_country_df.count())

# COMMAND ----------

int(movie_df.count()) * int(country_df.count())
