-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Scalar Functions(Funciones Escalares)

-- COMMAND ----------

USE movie_silver;

-- COMMAND ----------

SELECT *, CONCAT(title, ' - ', release_date) AS title_with_release_date
FROM movies;

-- COMMAND ----------

SELECT *, SPLIT(name, ' ')[0] forename, SPLIT(name, ' ')[2] surname
FROM persons;

-- COMMAND ----------

SELECT *, current_timestamp()
FROM movies;

-- COMMAND ----------

SELECT *, date_format(release_date, 'dd-MM-yyyy')
FROM movies;

-- COMMAND ----------

SELECT *, date_add(release_date, 1)
FROM movies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Aggregate Functions(Funciones de Agregación)

-- COMMAND ----------

SELECT COUNT(1)
FROM movies;

-- COMMAND ----------

SELECT MAX(release_date)
FROM movies;

-- COMMAND ----------

SELECT *
FROM movies
WHERE year_release_date = 2010;

-- COMMAND ----------

SELECT year_release_date, COUNT(1) AS total_count, SUM(budget) sum_budget,
  MAX(budget) max_budget, MIN(budget) min_budget, AVG(budget) avg_budget
FROM movies
GROUP BY year_release_date
HAVING COUNT(1) > 220
ORDER BY year_release_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Window Functions(Funciones de Ventana)

-- COMMAND ----------

SELECT title, year_release_date, release_date,
      RANK() OVER(PARTITION BY year_release_date ORDER BY release_date) AS rank
FROM movies
WHERE year_release_date IS NOT NULL
ORDER BY release_date;
