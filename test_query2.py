from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple

ActorFilm = namedtuple("ActorFilm", "actor actorid year film votes rating filmid")
Actor = namedtuple("Actor", "actor actorid year films quality_class years_since_film current_year is_active")
ActorHistorySCD = namedtuple("ActorHistorySCD", "actor quality_class is_active start_date end_date current_year")

def test_scd_generation_for_actor_films(spark):
    # Sample data for the actors table
    actor_data = [
        Actor("John Doe", "actor1", 2003, [], "good", 1, 2004, True),
        Actor("John Doe", "actor1", 2004, ["Film A"], "star", 0, 2004, True),
        Actor("Jane Smith", "actor2", 2003, [], "average", 2, 2004, True),
        Actor("Jane Smith", "actor2", 2004, ["Film B"], "good", 0, 2004, False),
        Actor("New Actor", "actor3", 2004, ["Film C"], "good", 0, 2004, True)
    ]

    # Sample data for the actor_films table (new films released in 2004)
    actor_films_data = [
        ActorFilm("John Doe", "actor1", 2004, "Film A", 100, 8.5, "film1"),
        ActorFilm("Jane Smith", "actor2", 2004, "Film B", 50, 7.0, "film2"),
        ActorFilm("New Actor", "actor3", 2004, "Film C", 60, 7.5, "film3")
    ]

    # Create DataFrames
    actor_df = spark.createDataFrame(actor_data)
    actor_films_df = spark.createDataFrame(actor_films_data)

    # Register the DataFrames as temporary views
    actor_df.createOrReplaceTempView("actors")
    actor_films_df.createOrReplaceTempView("actor_films")

    # Run the transformation
    actual_df = do_query2(spark, actor_df)

    # Define the expected output (based on the transformation logic)
    expected_data = [
        ActorHistorySCD("John Doe", "star", True, 2003, 2004, 2004),
        ActorHistorySCD("John Doe", "star", True, 2004, 2004, 2004),
        ActorHistorySCD("Jane Smith", "average", True, 2003, 2003, 2004),
        ActorHistorySCD("Jane Smith", "good", False, 2004, 2004, 2004),
        ActorHistorySCD("New Actor", "good", True, 2004, 2004, 2004)
    ]
    
    expected_df = spark.createDataFrame(expected_data)

    # Compare actual result with expected result
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

