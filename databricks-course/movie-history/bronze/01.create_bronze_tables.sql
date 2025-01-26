-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS movie_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Crear tablas para archivos CSV

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la tabla "movies"

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.movies;
CREATE TABLE IF NOT EXISTS movie_bronze.movies(
  movieId INT,
  title STRING,
  budget DOUBLE,
  homePage STRING,
  overview STRING,
  popularity DOUBLE,
  yearReleaseDate INT,
  releaseDate DATE,
  revenue DOUBLE,
  durationTime INT,
  movieStatus STRING,
  tagline STRING,
  voteAverage DOUBLE,
  voteCount INT
)
USING CSV
OPTIONS(path "/mnt/moviehistory/bronze/movie.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la tabla "languages"

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.languages;
CREATE TABLE IF NOT EXISTS movie_bronze.languages(
  languageId INT,
  languageCode STRING,
  languageName STRING
)
USING CSV
OPTIONS(path "/mnt/moviehistory/bronze/language.csv", header true)

-- COMMAND ----------

SELECT * FROM movie_bronze.languages;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la Tabla "genres"

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.genres;
CREATE TABLE IF NOT EXISTS movie_bronze.genres(
  genreId INT,
  genreName STRING
)
USING CSV
OPTIONS(path "/mnt/moviehistory/bronze/genre.csv", header true)

-- COMMAND ----------

SELECT * FROM movie_bronze.genres;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Crear tablas para archivos JSON

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la tabla "countries"
-- MAGIC   - JSON de una sola línea
-- MAGIC   - Estructura Simple

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.countries;
CREATE TABLE IF NOT EXISTS movie_bronze.countries(
  countryId INT,
  countryIsoCode STRING,
  countryName STRING
)
USING JSON
OPTIONS(path "/mnt/moviehistory/bronze/country.json")

-- COMMAND ----------

SELECT * FROM movie_bronze.countries;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la tabla "persons"
-- MAGIC   - JSON de una sola línea
-- MAGIC   - Estructura Compleja

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.persons;
CREATE TABLE IF NOT EXISTS movie_bronze.persons(
  personId INT,
  personName STRUCT<forename: STRING, surname: STRING>
)
USING JSON
OPTIONS(path "/mnt/moviehistory/bronze/person.json")

-- COMMAND ----------

SELECT * FROM movie_bronze.persons;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la tabla "movies_genres"
-- MAGIC   - JSON de una sola línea
-- MAGIC   - Estructura Simple

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.movies_genres;
CREATE TABLE IF NOT EXISTS movie_bronze.movies_genres(
  movieId INT,
  genreId INT
)
USING JSON
OPTIONS(path "/mnt/moviehistory/bronze/movie_genre.json")

-- COMMAND ----------

SELECT * FROM movie_bronze.movies_genres;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la tabla "movies_cats"
-- MAGIC   - JSON Multilínea
-- MAGIC   - Estructura Simple

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.movies_cats;
CREATE TABLE IF NOT EXISTS movie_bronze.movies_cats(
  movieId INT,
  personId INT,
  characterName STRING,
  genderId INT,
  castOrder INT
)
USING JSON
OPTIONS(path "/mnt/moviehistory/bronze/movie_cast.json", multiLine true)

-- COMMAND ----------

SELECT * FROM movie_bronze.movies_cats;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la tabla "languages_roles"
-- MAGIC   - JSON Multilínea
-- MAGIC   - Estructura Simple

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.languages_roles;
CREATE TABLE IF NOT EXISTS movie_bronze.languages_roles(
  roleId INT,
  languageRole STRING
)
USING JSON
OPTIONS(path "/mnt/moviehistory/bronze/language_role.json", multiLine true)

-- COMMAND ----------

SELECT * FROM movie_bronze.languages_roles;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Crear tablas para Lista de Archivos(CSVs y JSONs)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la tabla "productions_companies"
-- MAGIC   - Archivo CSV
-- MAGIC   - Múltiples Archivos

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.productions_companies;
CREATE TABLE IF NOT EXISTS movie_bronze.productions_companies(
  companyId INT,
  companyName STRING
)
USING CSV
OPTIONS(path "/mnt/moviehistory/bronze/production_company")

-- COMMAND ----------

SELECT * FROM movie_bronze.productions_companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la tabla "movies_companies"
-- MAGIC   - Archivo CSV
-- MAGIC   - Múltiples Archivos

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.movies_companies;
CREATE TABLE IF NOT EXISTS movie_bronze.movies_companies(
  movieId INT,
  companyId INT
)
USING CSV
OPTIONS(path "/mnt/moviehistory/bronze/movie_company")

-- COMMAND ----------

SELECT * FROM movie_bronze.movies_companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la tabla "movies_languages"
-- MAGIC   - Archivo JSON Multilínea
-- MAGIC   - Múltiples Archivos

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.movies_languages;
CREATE TABLE IF NOT EXISTS movie_bronze.movies_languages(
  movieId INT,
  languageId INT,
  languageRoleId INT
)
USING JSON
OPTIONS(path "/mnt/moviehistory/bronze/movie_language", multiLine true)

-- COMMAND ----------

SELECT * FROM movie_bronze.movies_languages;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Crear la tabla "productions_countries"
-- MAGIC   - Archivo JSON Multilínea
-- MAGIC   - Múltiples Archivos

-- COMMAND ----------

DROP TABLE IF EXISTS movie_bronze.productions_countries;
CREATE TABLE IF NOT EXISTS movie_bronze.productions_countries(
  movieId INT,
  countryId INT
)
USING JSON
OPTIONS(path "/mnt/moviehistory/bronze/production_country", multiLine true)

-- COMMAND ----------

SELECT * FROM movie_bronze.productions_countries;
