from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum

spark = SparkSession.builder.appName("bucket_join").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


def create_iceberg_bucketed_tables():
    spark.sql("""
     CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
         match_id STRING,
         player_gamertag STRING,
         player_total_kills INTEGER,
         player_total_deaths INTEGER
    )
     USING iceberg
     PARTITIONED BY (bucket(16, match_id));
     """)

    spark.sql("""
     CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
         match_id STRING,
         is_team_game BOOLEAN,
         playlist_id STRING,
         map_id STRING,
         completion_date TIMESTAMP
     )
     USING iceberg
     PARTITIONED BY (bucket(16, match_id));
     """)

    spark.sql("""
     CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
         match_id STRING,
         player_gamertag STRING,
         medal_id STRING,
         medal_name STRING,
         medal_count INTEGER
     )
     USING iceberg
     PARTITIONED BY (bucket(16, match_id));
     """)

    spark.sql("""
     CREATE TABLE IF NOT EXISTS bootcamp.joined_bucketed (
         match_id STRING,
         player_gamertag STRING,
         player_total_kills INTEGER,
         player_total_deaths INTEGER,
         is_team_game BOOLEAN,
         playlist_id STRING,
         completion_date TIMESTAMP,
         map_id STRING,
         medal_id STRING,
         medal_name STRING,
         medal_count INTEGER
     )
     USING iceberg
     PARTITIONED BY (bucket(16, match_id));
     """)

    spark.sql("""
     CREATE TABLE IF NOT EXISTS bootcamp.joined_bucketed_sorted (
         match_id STRING,
         player_gamertag STRING,
         player_total_kills INTEGER,
         player_total_deaths INTEGER,
         is_team_game BOOLEAN,
         playlist_id STRING,
         completion_date TIMESTAMP,
         map_id STRING,
         medal_id STRING,
         medal_name STRING,
         medal_count INTEGER
     )
     USING iceberg
     PARTITIONED BY (bucket(16, match_id));
     """)

create_iceberg_bucketed_tables()


match_details = spark.read.option("header", True).csv("/home/iceberg/data/match_details.csv").sort(col('match_id'))
matches = spark.read.option("header", True).csv("/home/iceberg/data/matches.csv").sort(col('match_id'))
medals_matches_players = spark.read.option("header", True).csv("/home/iceberg/data/medals_matches_players.csv").sort(col('match_id'))
medals = spark.read.option("header", True).csv("/home/iceberg/data/medals.csv")


match_details \
    .select('match_id', 'player_gamertag', 'player_total_kills', 'player_total_deaths') \
    .write.mode("overwrite").bucketBy(16, 'match_id').saveAsTable("bootcamp.match_details_bucketed")
matches \
    .select(col('match_id'), col('is_team_game'), col('playlist_id'), col('mapid').alias('map_id'), 'completion_date') \
    .write.mode('overwrite').bucketBy(16, 'match_id').saveAsTable('bootcamp.matches_bucketed')
medals_matches_players.alias('mmp') \
    .join(medals.alias('m'), on=col('mmp.medal_id') == col('m.medal_id'), how='inner') \
    .select(
        col('mmp.match_id'), 
        col('mmp.player_gamertag'), 
        col('mmp.medal_id'), 
        col('mmp.count').alias('medal_count'),
        col('m.name').alias('medal_name')
    ) \
    .write.mode("overwrite") \
    .bucketBy(16, 'match_id') \
    .saveAsTable("bootcamp.medals_matches_players_bucketed")


joined_df = spark.sql("""
  WITH joined AS (
    SELECT 
        md.*,
        mat.is_team_game,
        mat.playlist_id,
        mat.completion_date,
        mat.map_id,
        mmp.medal_id,
        mmp.medal_count,
        mmp.medal_name
    FROM bootcamp.match_details_bucketed md
    JOIN bootcamp.matches_bucketed mat ON md.match_id = mat.match_id
    JOIN bootcamp.medals_matches_players_bucketed mmp ON md.match_id = mmp.match_id
  )
  SELECT * FROM joined
""")

joined_df.show()


# Which player averages the most kills per game
result = (
    joined_df
    .groupBy('player_gamertag')
    .agg(avg('player_total_kills').alias('avg_total_player_kills'))
    .orderBy(col('avg_total_player_kills').desc())
)
print(result.first())


# Which playlist gets played the most?
result = (
    joined_df
    .groupBy('playlist_id')
    .count()
    .orderBy(col('count').desc())
)
print(result.first())


# Which map gets played the most?
result = (
    joined_df
    .groupBy('map_id')
    .count()
    .orderBy(col('count').desc())
)
print(result.first())


# Which map do players get the most Killing Spree medals on
result = (
    joined_df
    .where(col('medal_name') == 'Killing Spree')
    .groupBy('map_id')
    .agg(sum(col('medal_count')).alias('total_medals'))
    .orderBy(col('total_medals').desc())
)
print(result.first())


# Try different sortWithinPartitions to see which one results in the smallest size

# write unsorted table
joined_df \
    .write.mode('overwrite') \
    .bucketBy(16, 'match_id') \
    .saveAsTable('bootcamp.joined_bucketed')
res = spark.sql("SELECT SUM(file_size_in_bytes) as table_size FROM bootcamp.joined_bucketed.files")
res.show() # result 7284454 bytes


# sort table by player_gamertag
joined_df \
    .sortWithinPartitions(col('player_gamertag')) \
    .write.mode('overwrite') \
    .bucketBy(16, 'match_id') \
    .saveAsTable('bootcamp.joined_bucketed_sorted')
res = spark.sql("SELECT SUM(file_size_in_bytes) as table_size FROM bootcamp.joined_bucketed_sorted.files")
res.show() # result 8568487 bytes which is larger than joined_bucketed


# sort table by medal_name
joined_df \
    .sortWithinPartitions(col('medal_name')) \
    .write.mode('overwrite') \
    .bucketBy(16, 'match_id') \
    .saveAsTable('bootcamp.joined_bucketed_sorted')
res = spark.sql("SELECT SUM(file_size_in_bytes) as table_size FROM bootcamp.joined_bucketed_sorted.files")
res.show() # result 6573981 bytes which is smaller than joined_bucketed





