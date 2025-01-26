# Databricks notebook source
# MAGIC %md
# MAGIC ### Crear la Tabla results_movie en la capa "gold"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-30")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql("""
          CREATE TABLE IF NOT EXISTS movie_gold.results_movie
          (
              year_release_date INT,
              country_name STRING,
              company_name STRING,
              budget FLOAT,
              revenue FLOAT,
              movie_id INT,
              country_id INT,
              company_id INT,
              created_date DATE,
              updated_date DATE
          )
          USING DELTA
          """)

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW v_results_movie
            AS
            SELECT M.year_release_date, C.country_name, PCO.company_name,
                    M.budget, M.revenue, M.movie_id, C.country_id, PCO.company_id
            FROM movie_silver.movies M
            INNER JOIN movie_silver.productions_countries PC ON M.movie_id = PC.movie_id
            INNER JOIN movie_silver.countries C ON C.country_id = PC.country_id
            INNER JOIN movie_silver.movies_companies MC ON M.movie_id = MC.movie_id
            INNER JOIN movie_silver.productions_companies PCO ON MC.company_id = PCO.company_id
            WHERE M.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
            MERGE INTO movie_gold.results_movie tgt
            USING v_results_movie src
            ON (tgt.movie_id = src.movie_id AND tgt.country_id = src.country_id AND tgt.company_id = src.company_id)
            WHEN MATCHED THEN
            UPDATE SET
                tgt.year_release_date = src.year_release_date,
                tgt.country_name = src.country_name,
                tgt.company_name = src.company_name,
                tgt.budget = src.budget,
                tgt.revenue = src.revenue,
                tgt.updated_date = current_timestamp
            WHEN NOT MATCHED THEN 
            INSERT ( year_release_date, country_name, company_name, budget, revenue, 
                        movie_id, country_id, company_id, created_date)
            VALUES ( year_release_date, country_name, company_name, budget, revenue, 
                        movie_id, country_id, company_id, current_timestamp)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM v_results_movie;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM movie_gold.results_movie;
