# Databricks notebook source
# MAGIC %md
# MAGIC ###Spark Filter Transformation

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

movies_df = spark.read.parquet(f"{silver_folder_path}/movies")

# COMMAND ----------

movies_filtered_df = movies_df.filter("year_release_date = 2007")

# COMMAND ----------

movies_filtered_df = movies_df.filter(movies_df.year_release_date == 2007)

# COMMAND ----------

movies_filtered_df = movies_df.filter(movies_df["year_release_date"] == 2007)

# COMMAND ----------

movies_filtered_df = movies_df.filter("year_release_date = 2007 and vote_average > 7")

# COMMAND ----------

movies_filtered_df = movies_df.filter((movies_df.year_release_date == 2007) & (movies_df.vote_average > 7))

# COMMAND ----------

movies_filtered_df = movies_df.filter((movies_df["year_release_date"] == 2007) & (movies_df["vote_average"] > 7))

# COMMAND ----------

movies_filtered_df = movies_df.where((movies_df["year_release_date"] == 2007) & (movies_df["vote_average"] > 7))

# COMMAND ----------

display(movies_filtered_df)
