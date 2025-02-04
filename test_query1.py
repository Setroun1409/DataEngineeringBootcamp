from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple

ActorHistorySCD = namedtuple("ActorHistorySCD", "actor quality_class is_active start_date end_date current_year")
Actor = namedtuple("Actor", "actor quality_class is_active current_year")

def test_scd_generation_for_actors(spark):
    # Define the source data for the actors' history and the current year data.
    actor_history_data = [
        ActorHistorySCD("John Doe", "Good", True, 2000, 2002, 2004),
        ActorHistorySCD("John Doe", "Bad", False, 2003, 2003, 2004),
        ActorHistorySCD("Jane Smith", "Good", True, 2000, 2004, 2004),
    ]
    
    actors_data = [
        Actor("John Doe", "Good", True, 2004),
        Actor("Jane Smith", "Bad", False, 2004),
        Actor("New Actor", "Good", True, 2004)
    ]

    # Convert the data into DataFrames
    actor_history_df = spark.createDataFrame(actor_history_data)
    actors_df = spark.createDataFrame(actors_data)

    # Register the DataFrames as temporary views
    actor_history_df.createOrReplaceTempView("actors_history_scd")
    actors_df.createOrReplaceTempView("actors")

    # Run the transformation
    actual_df = do_query1(spark, actors_df)

    # Define the expected output
    expected_data = [
        ActorHistorySCD("John Doe", "Good", True, 2000, 2002, 2022),
        ActorHistorySCD("John Doe", "Bad", False, 2003, 2003, 2022),
        ActorHistorySCD("Jane Smith", "Good", True, 2000, 2004, 2022),
        ActorHistorySCD("Jane Smith", "Bad", False, 2004, 2004, 2022),
        ActorHistorySCD("New Actor", "Good", True, 2004, 2004, 2022)
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Compare actual result with expected result
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

