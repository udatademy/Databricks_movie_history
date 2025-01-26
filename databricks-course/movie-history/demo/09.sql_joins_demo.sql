-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Join

-- COMMAND ----------

USE movie_silver;

-- COMMAND ----------

DESC movies;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_movies_2011
AS
SELECT movie_id, title, budget, revenue, year_release_date, release_date
FROM movies
WHERE year_release_date = 2011

-- COMMAND ----------

SELECT * FROM v_movies_2011;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_productions_countries
AS
SELECT * FROM productions_countries;

-- COMMAND ----------

SELECT * FROM v_productions_countries;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_countries
AS
SELECT country_id, country_name FROM countries;

-- COMMAND ----------

SELECT * FROM v_countries;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### INNER JOIN

-- COMMAND ----------

SELECT VM.title, VM.budget, VM.revenue, VM.year_release_date, VM.release_date, VC.country_name
FROM v_movies_2011 VM
INNER JOIN v_productions_countries VPC ON VM.movie_id = VPC.movie_id
INNER JOIN v_countries VC ON VC.country_id = VPC.country_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### LEFT JOIN

-- COMMAND ----------

SELECT VM.title, VM.budget, VM.revenue, VM.year_release_date, VM.release_date, VC.country_name
FROM v_movies_2011 VM
LEFT JOIN v_productions_countries VPC ON VM.movie_id = VPC.movie_id
LEFT JOIN v_countries VC ON VC.country_id = VPC.country_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### RIGHT JOIN

-- COMMAND ----------

SELECT VM.title, VM.budget, VM.revenue, VM.year_release_date, VM.release_date, VC.country_name
FROM v_movies_2011 VM
RIGHT JOIN v_productions_countries VPC ON VM.movie_id = VPC.movie_id
RIGHT JOIN v_countries VC ON VC.country_id = VPC.country_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### FULL JOIN

-- COMMAND ----------

SELECT VM.title, VM.budget, VM.revenue, VM.year_release_date, VM.release_date, VC.country_name
FROM v_movies_2011 VM
FULL JOIN v_productions_countries VPC ON VM.movie_id = VPC.movie_id
FULL JOIN v_countries VC ON VC.country_id = VPC.country_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SEMI JOIN

-- COMMAND ----------

SELECT VM.title, VM.budget, VM.revenue, VM.year_release_date, VM.release_date
FROM v_movies_2011 VM
INNER JOIN v_productions_countries VPC ON VM.movie_id = VPC.movie_id
SEMI JOIN v_countries VC ON VC.country_id = VPC.country_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ANTI JOIN

-- COMMAND ----------

SELECT VM.title, VM.budget, VM.revenue, VM.year_release_date, VM.release_date
FROM v_movies_2011 VM
INNER JOIN v_productions_countries VPC ON VM.movie_id = VPC.movie_id
ANTI JOIN v_countries VC ON VC.country_id = VPC.country_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CROSS JOIN

-- COMMAND ----------

SELECT VM.title, VM.budget, VM.revenue, VM.year_release_date, VM.release_date, VC.country_name
FROM v_movies_2011 VM CROSS JOIN v_countries VC;
