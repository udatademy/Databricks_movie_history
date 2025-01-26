-- Databricks notebook source
USE movie_gold;

-- COMMAND ----------

SELECT country_name,
      COUNT(country_name) AS total_movie,
      SUM(budget) AS total_budget,
      CAST(AVG(budget) AS DECIMAL(18,2)) AS avg_budget,
      SUM(revenue) AS total_revenue,
      CAST(AVG(revenue) AS DECIMAL(18,2)) AS avg_revenue
FROM results_movie
GROUP BY country_name
ORDER BY total_revenue DESC
LIMIT 10;

-- COMMAND ----------

SELECT country_name,
      COUNT(country_name) AS total_movie,
      SUM(budget) AS total_budget,
      CAST(AVG(budget) AS DECIMAL(18,2)) AS avg_budget,
      SUM(revenue) AS total_revenue,
      CAST(AVG(revenue) AS DECIMAL(18,2)) AS avg_revenue
FROM results_movie
WHERE year_release_Date BETWEEN 2010 AND 2015
GROUP BY country_name
ORDER BY total_revenue DESC
LIMIT 10;
