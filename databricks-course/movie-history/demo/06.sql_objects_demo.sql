-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Base de Datos(Database)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Objetivos
-- MAGIC   1. Documentación sobre Spark SQL
-- MAGIC   2. Crear la Base de Datos "demo"
-- MAGIC   3. Acceder al **"Catalog"** en la "Intefaz de Usuario"
-- MAGIC   4. Comando **"SHOW"**
-- MAGIC   5. Comando **DESCRIBE(DESC)**
-- MAGIC   6. Mostrar la Base de Datos Actual

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tablas Administradas(Managed Tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Objetivos
-- MAGIC   1. Crear una **"Tabla Adminstrada(Managed Table)"** con Python
-- MAGIC   2. Crear una **"Tabla Adminstrada(Managed Table)"** con SQL
-- MAGIC   3. Efecto de eliminar una Tabla Administrada
-- MAGIC   4. Describir(Describe) la Tabla

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_movie_genre_language = spark.read.parquet(f"{gold_folder_path}/results_movie_genre_language")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_movie_genre_language.write.format("parquet").saveAsTable("demo.results_movie_genre_language_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED results_movie_genre_language_python;

-- COMMAND ----------

CREATE TABLE demo.results_movie_genre_language_sql
AS
SELECT *
FROM results_movie_genre_language_python
WHERE genre_name = 'Adventure'

-- COMMAND ----------

SELECT * FROM results_movie_genre_language_sql

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

DESCRIBE EXTENDED results_movie_genre_language_sql;

-- COMMAND ----------

DROP TABLE IF EXISTS results_movie_genre_language_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tablas Externas(External Tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Objetivos
-- MAGIC   1. Crear una **"Tabla Externa(External Table)"** con Python
-- MAGIC   2. Crear una **"Tabla Externa(External Table)"** con SQL
-- MAGIC   3. Efecto de eliminación de una **"Tabla Externa(External Table)"**
-- MAGIC   4. Describir(Describe) la Tabla

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_movie_genre_language.write.format("parquet").option("path", f"{gold_folder_path}/results_movie_genre_language_py").saveAsTable("demo.results_movie_genre_language_py")

-- COMMAND ----------

DESC EXTENDED demo.results_movie_genre_language_py

-- COMMAND ----------

CREATE TABLE demo.results_movie_genre_language_sql(
  title STRING,
  duration_time INT,
  release_date DATE,
  vote_average FLOAT,
  language_name STRING,
  genre_name STRING,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/moviehistory/gold/results_movie_genre_language_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.results_movie_genre_language_sql
SELECT * FROM demo.results_movie_genre_language_py
WHERE genre_name = 'Adventure'

-- COMMAND ----------

SELECT COUNT(1)
FROM demo.results_movie_genre_language_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DROP TABLE demo.results_movie_genre_language_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Vistas(Views)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Objetivos
-- MAGIC   1. Crear Vista Temporal
-- MAGIC   2. Crear Vista Temporal Global
-- MAGIC   1. Crear Vista Permanente

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_results_movies_genres_language
AS
SELECT *
FROM demo.results_movie_genre_language_py
WHERE genre_name = 'Adventure';

-- COMMAND ----------

SELECT * FROM v_results_movies_genres_language

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_results_movies_genres_language
AS
SELECT *
FROM demo.results_movie_genre_language_py
WHERE genre_name = 'Drama';

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SELECT * FROM global_temp.gv_results_movies_genres_language;

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_results_movies_genres_language
AS
SELECT *
FROM demo.results_movie_genre_language_py
WHERE genre_name = 'Comedy';

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM pv_results_movies_genres_language;
