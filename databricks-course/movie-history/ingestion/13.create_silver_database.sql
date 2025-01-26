-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS movie_silver
LOCATION "/mnt/moviehistory/silver";

-- COMMAND ----------

DESC DATABASE movie_silver;
