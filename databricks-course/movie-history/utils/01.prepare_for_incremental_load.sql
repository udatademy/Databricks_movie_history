-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Eliminar y volver a crear todas las Bases de Datos

-- COMMAND ----------

DROP DATABASE IF EXISTS movie_silver CASCADE;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movie_silver
LOCATION "/mnt/moviehistory/silver";

-- COMMAND ----------

DROP DATABASE IF EXISTS movie_gold CASCADE;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movie_gold
LOCATION "/mnt/moviehistory/gold";
