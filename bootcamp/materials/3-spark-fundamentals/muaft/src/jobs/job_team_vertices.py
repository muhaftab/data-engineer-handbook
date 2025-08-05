from pyspark.sql import SparkSession, DataFrame

query = """
WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id) as row_num
    FROM teams
)
SELECT
    team_id AS identifier,
    'team' AS `type`,
    map(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', CAST(yearfounded AS STRING)
        ) AS properties
FROM teams_deduped
WHERE row_num = 1
"""


def do_team_vertices_transformation(spark: SparkSession, df: DataFrame):
    df.createOrReplaceTempView("teams")
    return spark.sql(query)


def main():
    spark = SparkSession.builder.appName("bootcamp").master("local[*]").getOrCreate()
    try:
        input_df = spark.table("teams")
        output_df = do_team_vertices_transformation(spark, input_df)
        output_df.write.mode("overwrite").insertInto("vertices")
        
        res_df = spark.sql("select * from vertices")
        res_df.show()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
