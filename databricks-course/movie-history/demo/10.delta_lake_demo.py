# Databricks notebook source
# MAGIC %md
# MAGIC ### Read & Write en Delta Lake
# MAGIC 1. Escribir datos en Delta Lake(Managed Table)
# MAGIC 2. Escribir datos en Delta Lake(External Table)
# MAGIC 3. Leer datos desde Delta Lake(Table)
# MAGIC 4. Leer datos desde Delta Lake(File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS movie_demo
# MAGIC LOCATION "/mnt/moviehistory/demo"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

# COMMAND ----------

movie_schema = StructType( fields= [
    StructField("movieId", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("budget", DoubleType(), True),
    StructField("homePage", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("yearReleaseDate", IntegerType(), True),
    StructField("releaseDate", DateType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("durationTime", IntegerType(), True),
    StructField("movieStatus", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("voteAverage", DoubleType(), True),
    StructField("voteCount", IntegerType(), True)
] )

# COMMAND ----------

movie_df = spark.read \
            .option("header", True) \
            .schema(movie_schema) \
            .csv("/mnt/moviehistory/bronze/2024-12-30/movie.csv")

# COMMAND ----------

movie_df.write.format("delta").mode("overwrite").saveAsTable("movie_demo.movies_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_managed;

# COMMAND ----------

movie_df.write.format("delta").mode("overwrite").save("/mnt/moviehistory/demo/movies_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE movie_demo.movies_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/moviehistory/demo/movies_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_external;

# COMMAND ----------

movies_external_df = spark.read.format("delta").load("/mnt/moviehistory/demo/movies_external")

# COMMAND ----------

display(movies_external_df)

# COMMAND ----------

movie_df.write.format("delta").mode("overwrite").partitionBy("yearReleaseDate").saveAsTable("movie_demo.movies_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS movie_demo.movies_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update & Delete en Delta Lake
# MAGIC 1. Update desde Delta Lake
# MAGIC 2. Delete desde Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE movie_demo.movies_managed
# MAGIC SET durationTime = 60
# MAGIC WHERE yearReleaseDate = 2012;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_managed
# MAGIC WHERE yearReleaseDate = 2012;

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/mnt/moviehistory/demo/movies_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "yearReleaseDate = 2013",
  set = { "durationTime": "100" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_managed
# MAGIC WHERE yearReleaseDate = 2013;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM movie_demo.movies_managed
# MAGIC WHERE yearReleaseDate = 2014;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_managed
# MAGIC WHERE yearReleaseDate = 2014;

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/mnt/moviehistory/demo/movies_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("yearReleaseDate = 2015")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_managed
# MAGIC WHERE yearReleaseDate = 2015;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge / Upsert en Delta Lake

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

# COMMAND ----------

movie_schema = StructType( fields= [
    StructField("movieId", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("budget", DoubleType(), True),
    StructField("homePage", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("yearReleaseDate", IntegerType(), True),
    StructField("releaseDate", DateType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("durationTime", IntegerType(), True),
    StructField("movieStatus", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("voteAverage", DoubleType(), True),
    StructField("voteCount", IntegerType(), True)
] )

# COMMAND ----------

movies_day1_df = spark.read \
                .option("header", True) \
                .schema(movie_schema) \
                .csv("/mnt/moviehistory/bronze/2024-12-30/movie.csv") \
                .filter("yearReleaseDate < 2000") \
                .select("movieId", "title", "yearReleaseDate", "releaseDate", "durationTime")

# COMMAND ----------

display(movies_day1_df)

# COMMAND ----------

movies_day1_df.createOrReplaceTempView("movies_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

movies_day2_df = spark.read \
                .option("header", True) \
                .schema(movie_schema) \
                .csv("/mnt/moviehistory/bronze/2024-12-30/movie.csv") \
                .filter("yearReleaseDate BETWEEN 1998 AND 2005") \
                .select("movieId", upper("title").alias("title"), "yearReleaseDate", "releaseDate", "durationTime")

# COMMAND ----------

display(movies_day2_df)

# COMMAND ----------

movies_day2_df.createOrReplaceTempView("movies_day2")

# COMMAND ----------

from pyspark.sql.functions import upper

movies_day3_df = spark.read \
                .option("header", True) \
                .schema(movie_schema) \
                .csv("/mnt/moviehistory/bronze/2024-12-30/movie.csv") \
                .filter("yearReleaseDate BETWEEN 1983 AND 1998 OR yearReleaseDate BETWEEN 2006 AND 2010") \
                .select("movieId", upper("title").alias("title"), "yearReleaseDate", "releaseDate", "durationTime")

# COMMAND ----------

display(movies_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS movie_demo.movies_merge(
# MAGIC   movieId INT,
# MAGIC   title STRING,
# MAGIC   yearReleaseDate INT,
# MAGIC   releaseDate DATE,
# MAGIC   durationTime INT,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Día 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO movie_demo.movies_merge tgt
# MAGIC USING movies_day1 src
# MAGIC ON tgt.movieId = src.movieId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.title = src.title,
# MAGIC     tgt.yearReleaseDate = src.yearReleaseDate,
# MAGIC     tgt.releaseDate = src.releaseDate,
# MAGIC     tgt.durationTime = src.durationTime,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT ( movieId, title, yearReleaseDate, releaseDate, durationTime, createdDate)
# MAGIC   VALUES ( movieId, title, yearReleaseDate, releaseDate, durationTime, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Día 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO movie_demo.movies_merge tgt
# MAGIC USING movies_day2 src
# MAGIC ON tgt.movieId = src.movieId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.title = src.title,
# MAGIC     tgt.yearReleaseDate = src.yearReleaseDate,
# MAGIC     tgt.releaseDate = src.releaseDate,
# MAGIC     tgt.durationTime = src.durationTime,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT ( movieId, title, yearReleaseDate, releaseDate, durationTime, createdDate)
# MAGIC   VALUES ( movieId, title, yearReleaseDate, releaseDate, durationTime, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Día 3

# COMMAND ----------

from delta.tables import *

deltaTablePeople = DeltaTable.forPath(spark, '/mnt/moviehistory/demo/movies_merge')


deltaTablePeople.alias('tgt') \
  .merge(
    movies_day3_df.alias('src'),
    'tgt.movieId = src.movieId'
  ) \
  .whenMatchedUpdate(set =
    {
      "tgt.title": "src.title",
      "tgt.yearReleaseDate": "src.yearReleaseDate",
      "tgt.releaseDate": "src.releaseDate",
      "tgt.durationTime": "src.durationTime",
      "tgt.updatedDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "movieId": "movieId",
      "title": "title",
      "yearReleaseDate": "yearReleaseDate",
      "releaseDate": "releaseDate",
      "durationTime": "durationTime",
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC ### History, Time Travel y Vacuum
# MAGIC 1. Historia y Control de Versiones
# MAGIC 2. Viaje en el Tiempo
# MAGIC 3. Vacio

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movies_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_merge TIMESTAMP AS OF '2025-01-19T23:14:40.000+00:00'

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2025-01-19T23:14:40.000+00:00').load("/mnt/moviehistory/demo/movies_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM movie_demo.movies_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_merge TIMESTAMP AS OF '2025-01-19T23:14:40.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM movie_demo.movies_merge RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_merge TIMESTAMP AS OF '2025-01-19T23:14:40.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movies_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM movie_demo.movies_merge 
# MAGIC WHERE yearReleaseDate = 2004;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_merge ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movies_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_merge VERSION AS OF 9;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO movie_demo.movies_merge tgt
# MAGIC USING movie_demo.movies_merge VERSION AS OF 9 src
# MAGIC ON tgt.movieId = src.movieId
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movies_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaction Log en Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS movie_demo.movies_log(
# MAGIC   movieId INT,
# MAGIC   title STRING,
# MAGIC   yearReleaseDate INT,
# MAGIC   releaseDate DATE,
# MAGIC   durationTime INT,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movies_log;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO movie_demo.movies_log
# MAGIC SELECT * FROM movie_demo.movies_merge
# MAGIC WHERE movieId = 125537;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_demo.movies_log

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movies_log

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO movie_demo.movies_log
# MAGIC SELECT * FROM movie_demo.movies_merge
# MAGIC WHERE movieId = 133575;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movies_log

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM movie_demo.movies_log
# MAGIC WHERE movieId = 125537

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY movie_demo.movies_log

# COMMAND ----------

list = [118452, 124606, 125052, 125123, 125263, 125537, 126141, 133575, 142132, 146269, 157185]
for movieId in list:
    spark.sql(f"""INSERT INTO movie_demo.movies_log
              SELECT * FROM movie_demo.movies_merge 
              WHERE movieId = {movieId}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO movie_demo.movies_log
# MAGIC SELECT * FROM movie_demo.movies_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convertir formato "Parquet" a "Delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS movie_demo.movies_convert_to_delta(
# MAGIC   movieId INT,
# MAGIC   title STRING,
# MAGIC   yearReleaseDate INT,
# MAGIC   releaseDate DATE,
# MAGIC   durationTime INT,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO movie_demo.movies_convert_to_delta
# MAGIC SELECT * FROM movie_demo.movies_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA movie_demo.movies_convert_to_delta

# COMMAND ----------

df = spark.table("movie_demo.movies_convert_to_delta")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("parquet").save("/mnt/moviehistory/demo/movies_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/moviehistory/demo/movies_convert_to_delta_new`
