# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark Aggregate Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Funciones simples de agregación

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

movies_df = spark.read.parquet(f"{silver_folder_path}/movies")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

movies_df.select(count("*")).show()

# COMMAND ----------

movies_df.select(count("year_release_date")).show()

# COMMAND ----------

movies_df.select(countDistinct("year_release_date")).show()

# COMMAND ----------

display(movies_df.select(sum("budget")))

# COMMAND ----------

movies_df.filter("year_release_date = 2016") \
        .select(sum("budget"), count("movie_id")) \
        .withColumnRenamed("sum(budget)", "total_budget") \
        .withColumnRenamed("count(movie_id)", "count_movies") \
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group By

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, max, min, avg

# COMMAND ----------

movie_grup_by_df = movies_df \
.groupBy("year_release_date") \
    .agg(
        sum("budget").alias("total_budget"),
        avg("budget").alias("avg_budget"),
        max("budget").alias("max_budget"),
        min("budget").alias("min_budget"),
        count("movie_id").alias("count_movies")
    )

# COMMAND ----------

display(movie_grup_by_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, dense_rank

# COMMAND ----------

movie_rank = Window.partitionBy("year_release_date").orderBy(desc("budget"))
movie_dense_rank = Window.partitionBy("year_release_date").orderBy(desc("budget"))

movies_df.select("title", "budget", "year_release_date") \
        .filter("year_release_date is not null") \
        .withColumn("rank", rank().over(movie_rank)) \
        .withColumn("dense_rank", dense_rank().over(movie_dense_rank)) \
        .display()
