from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, avg, desc, sum

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("BroadcastJoin") \
    .getOrCreate()

# Disable auto broadcast join threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Read the CSV files
medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")
matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")

# Explicit Broadcast Join for medals and maps tables
broadcastJoin = medals.join(broadcast(maps), medals["mapid"] == maps["mapid"])

# Bucket join for matches, match_details, and medals_matches_players
matches.write.mode("append").partitionBy("completion_date").bucketBy(16, "match_id").saveAsTable("matches_bucketed")
match_details.write.mode("append").bucketBy(16, "match_id").saveAsTable("match_details_bucketed")
medals_matches_players.write.mode("append").bucketBy(16, "match_id").saveAsTable("medals_matches_players_bucketed")

# Perform the join between bucketed DataFrames using match_id as key
joinedDF = spark.sql("""
    SELECT a.match_id, a.is_team_game, a.playlist_id, a.completion_date, b.player_gamertag, 
           b.player_total_kills, b.player_total_deaths, c.medal_type, d.mapid, d.name AS map_name
    FROM matches_bucketed a
    JOIN match_details_bucketed b ON a.match_id = b.match_id
    JOIN medals_matches_players_bucketed c ON a.match_id = c.match_id
    JOIN maps d ON a.mapid = d.mapid
""")

# Show the joined result
joinedDF.show()

# Save the joined DataFrame
joinedDF.write.mode("append").partitionBy("completion_date").bucketBy(16, "match_id").saveAsTable("joined_matches")

# Aggregations

# Query 4a: Which player averages the most kills per game?
playerAvgKillsDF = joinedDF.groupBy("player_gamertag") \
    .agg(avg("player_total_kills").alias("avg_kills_per_game")) \
    .orderBy(desc("avg_kills_per_game"))
playerAvgKillsDF.show()

# Query 4b: Which playlist gets played the most?
mostPlayedPlaylistDF = joinedDF.groupBy("playlist_id") \
    .count() \
    .orderBy(desc("count"))
mostPlayedPlaylistDF.show()

# Query 4c: Which map is played the most? (mapid included)
mostPlayedMapDF = joinedDF.groupBy("mapid", "map_name") \
    .count() \
    .orderBy(desc("count"))
mostPlayedMapDF.show()

# Query 4d: Which map has the highest number of Killing Spree medals?
mapWithMostKillingSpreeDF = joinedDF.groupBy("mapid", "map_name") \
    .agg(sum("medal_type == 'Killing Spree'").alias("total_killing_spree_medals")) \
    .orderBy(desc("total_killing_spree_medals"))
mapWithMostKillingSpreeDF.show()

# Optimization: Applying sortWithinPartitions for aggregations

# Query 5a: Player average kills with sortWithinPartitions optimization
playerAvgKillsDF = joinedDF.groupBy("player_gamertag") \
    .agg(avg("player_total_kills").alias("avg_kills_per_game")) \
    .sortWithinPartitions(desc("avg_kills_per_game"))
playerAvgKillsDF.show()

# Query 5b: Most played playlists with sortWithinPartitions optimization
mostPlayedPlaylistDF = joinedDF.groupBy("playlist_id") \
    .count() \
    .sortWithinPartitions(desc("count"))
mostPlayedPlaylistDF.show()

# Query 5c: Map with the most Killing Spree medals with sortWithinPartitions optimization
mapWithMostKillingSpreeDF = joinedDF.groupBy("mapid", "map_name") \
    .agg(sum("medal_type == 'Killing Spree'").alias("total_killing_spree_medals")) \
    .sortWithinPartitions(desc("total_killing_spree_medals"))
mapWithMostKillingSpreeDF.show()
