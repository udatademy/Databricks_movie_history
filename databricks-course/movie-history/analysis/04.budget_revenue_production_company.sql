-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Tarea

-- COMMAND ----------

USE movie_gold;

-- COMMAND ----------

SELECT company_name,
        COUNT(company_name) AS total_company_name,
        SUM(budget) AS total_budget,
        CAST(AVG(budget) AS DECIMAL(18,2)) AS avg_budget,
        SUM(revenue) AS total_revenue,
        CAST(AVG(revenue) AS DECIMAL(18,2)) AS avg_revenue
FROM results_movie
GROUP BY company_name
ORDER BY total_revenue DESC
LIMIT 10;

-- COMMAND ----------

SELECT company_name,
        COUNT(company_name) AS total_company_name,
        SUM(budget) AS total_budget,
        CAST(AVG(budget) AS DECIMAL(18,2)) AS avg_budget,
        SUM(revenue) AS total_revenue,
        CAST(AVG(revenue) AS DECIMAL(18,2)) AS avg_revenue
FROM results_movie
WHERE year_release_date BETWEEN 2010 AND 2015
GROUP BY company_name
ORDER BY total_revenue DESC
LIMIT 10;
