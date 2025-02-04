from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# Query with corrections for Spark
query = """
WITH last_date_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year = 2004
    AND end_date = 2004
),
historical_scd AS (
    SELECT
        actor,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM actors_history_scd
    WHERE current_year = 2004
    AND end_date < 2004
),
this_date_data AS (
    SELECT * FROM actors
    WHERE current_year = 2004
),
unchanged_records AS (
    SELECT
        td.actor,
        td.quality_class,
        td.is_active,
        ld.start_date,
        td.current_year AS end_date
    FROM this_date_data td
    JOIN last_date_scd ld
    ON ld.actor = td.actor
    WHERE td.quality_class = ld.quality_class
    AND td.is_active = ld.is_active
),
changed_records AS (
    SELECT
        td.actor,
        ld.quality_class AS old_quality_class,
        ld.is_active AS old_is_active,
        ld.start_date AS old_start_date,
        ld.end_date AS old_end_date,
        td.quality_class AS new_quality_class,
        td.is_active AS new_is_active,
        td.current_year AS new_start_date,
        td.current_year AS new_end_date
    FROM this_date_data td
    LEFT JOIN last_date_scd ld
    ON ld.actor = td.actor
    WHERE td.quality_class <> ld.quality_class
    OR td.is_active <> ld.is_active
),
new_records AS (
    SELECT
        td.actor,
        td.quality_class,
        td.is_active,
        td.current_year AS start_date,
        td.current_year AS end_date
    FROM this_date_data td
    LEFT JOIN last_date_scd ld
    ON td.actor = ld.actor
    WHERE ld.actor IS NULL
)

SELECT *, 2022 AS current_year FROM (
    SELECT * FROM historical_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL
    SELECT actor, old_quality_class AS quality_class, old_is_active AS is_active, old_start_date AS start_date, old_end_date AS end_date, new_quality_class AS quality_class, new_is_active AS is_active, new_start_date AS start_date, new_end_date AS end_date FROM changed_records
    UNION ALL
    SELECT * FROM new_records
) a
"""

def do_query1(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("players_scd") \
        .getOrCreate()
    
    # Assuming 'actors' DataFrame is loaded into the Spark session
    output_df = do_query1(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_1")

