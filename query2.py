from pyspark.sql import SparkSession

query = """
-- 1. Define a films structure using a Spark SQL approach
CREATE OR REPLACE TEMP VIEW films AS 
SELECT
    film,
    votes,
    rating,
    filmid
FROM some_source -- Replace with an actual source or temp view

-- 2. Define quality_class ENUM as a list of values (handled by Spark SQL)
CREATE OR REPLACE TEMP VIEW quality_class AS 
SELECT 'star' AS quality_class
UNION ALL SELECT 'good'
UNION ALL SELECT 'average'
UNION ALL SELECT 'bad'

-- 3. Create actors table using a Spark SQL approach, not relying on PostgreSQL types
CREATE OR REPLACE TEMP VIEW actors AS 
SELECT 
    actor,
    actorid,
    year,
    films,
    quality_class,
    years_since_film,
    current_year,
    is_active
FROM another_source -- Replace with an actual source or temp view

-- 4. Insert logic adjusted for PySpark compatibility
WITH yesterday_date AS (
    SELECT * FROM actors WHERE current_year = 2003
),
today_date AS (
    SELECT * FROM actor_films WHERE year = 2004
)

SELECT 
    COALESCE(t.actor, y.actor) AS actor,
    COALESCE(t.actorid, y.actorid) AS actorid,
    COALESCE(t.year, y.year) AS year,
    CASE 
        WHEN y.films IS NULL THEN ARRAY(t.film, t.votes, t.rating, t.filmid)
        WHEN t.year IS NOT NULL THEN y.films + ARRAY(t.film, t.votes, t.rating, t.filmid)
        ELSE y.films
    END AS films,
    CASE 
        WHEN t.year IS NOT NULL THEN 
            CASE WHEN t.rating > 8 THEN 'star'
                WHEN t.rating > 7 THEN 'good'
                WHEN t.rating > 6 THEN 'average'
                ELSE 'bad'
            END
        ELSE y.quality_class
    END AS quality_class,
    CASE WHEN t.year IS NOT NULL THEN 0
        ELSE y.years_since_film + 1
    END AS years_since_film,
    COALESCE(t.year, y.current_year + 1) AS current_year,
    CASE 
        WHEN t.film IS NOT NULL THEN TRUE
        ELSE FALSE 
    END AS is_active
FROM today_date t 
FULL OUTER JOIN yesterday_date y 
ON t.actorid = y.actorid
ON CONFLICT (actorid, year) DO NOTHING

-- 5. Create a history table for tracking SCD (Slowly Changing Dimensions)
CREATE OR REPLACE TEMP VIEW actors_history_scd AS
SELECT 
    actor,
    quality_class,
    is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    current_year
FROM actors
GROUP BY actor, quality_class, is_active, current_year
"""

def do_query2(spark, dataframe):
    dataframe.createOrReplaceTempView("actors2")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("players_scd") \
        .getOrCreate()
    output_df = do_query2(spark, spark.table("actors2"))
    output_df.write.mode("overwrite").insertInto("actors_2")
