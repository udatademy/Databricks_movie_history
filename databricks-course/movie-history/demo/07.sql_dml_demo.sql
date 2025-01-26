-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

USE movie_silver;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM movies
LIMIT 100;

-- COMMAND ----------

DESC movies;

-- COMMAND ----------

SELECT *
FROM movies
WHERE year_release_date = 2015 AND vote_average >= 7.5
ORDER BY vote_average DESC;
