from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

if __name__ == "__main__":
    spark = SparkSession.builder.appName("broadcast_join").getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    data_path = "/mnt/c/Users/muaft/source/github/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data"
    matches = spark.read.option("header", True).csv(data_path + "/matches.csv")
    medals_matches_players = spark.read.option("header", True).csv(data_path + "/medals_matches_players.csv")
    medals = spark.read.option("header", True).csv(data_path + "/medals.csv")
    maps = spark.read.option("header", True).csv(data_path + "/maps.csv")

    matches_and_medals = (
        medals.alias('me')
        .join(
            broadcast(medals_matches_players.alias('mmp')),
            on=col("me.medal_id") == col("mmp.medal_id"), 
            how="inner"
        )
        .withColumnRenamed("name", "medal_name")
    )                            

    matches_and_maps = (
        matches.alias("ma")
        .join(
            broadcast(maps.alias("m")),
            on=col("ma.mapid") == col("m.mapid"),
            how="inner"
        )
        .withColumnRenamed("mapid", "map_id")
    )
            
    
    medals_and_maps = (
        matches_and_medals.alias("mame")
        .join(
            broadcast(matches_and_maps.alias("mama")),
            on=col("mame.match_id") == col("mama.match_id"),
            how="inner"
        )
        .select(
            col('medal_id'),
            col('mdeal_name'),
            col('description'),
            col('difficulty'),
            col('match_id'),
            col('player_gamertag'),
            col('medal_count'),
            col('map_id'),
            col('name').alias('map_name'),
            col('playlist_id'),
            col('completion_date')
        )
    )
    medals_and_maps.show()