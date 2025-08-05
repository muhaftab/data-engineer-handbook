from collections import namedtuple
from enum import Enum
from chispa.dataframe_comparer import assert_df_equality
from src.jobs.actors_scd_job import generate_actors_history_scd

class QualityClass(Enum):
   STAR = 'star' 
   GOOD = 'good'
   AVERAGE = 'average'
   BAD = 'bad'

# actor TEXT, films film[], quality_class quality_class, is_active BOOLEAN, year INT,
Actors = namedtuple("Actors", "actor quality_class is_active year")

# actor TEXT, quality_class quality_class, is_active BOOLEAN, start_year INT, end_year INT, current_year INT
ActorHistoryScd = namedtuple("ActorsHistoryScd", "actor quality_class is_active start_year end_year current_year")

def test_scd_generation(spark):
    # arrnage
    input_data = [
        Actors("Tom Hanks", QualityClass.STAR.name, True, 1994),
        Actors("Tom Hanks", QualityClass.GOOD.name, False, 1997),
        Actors("Tom Hanks", QualityClass.STAR.name, True, 2000),
        Actors("Tom Hanks", QualityClass.STAR.name, True, 2001),
        Actors("Tom Hanks", QualityClass.STAR.name, True, 2003),
        Actors("Steven Segal", QualityClass.BAD.name, True, 1995),
        Actors("Steven Segal", QualityClass.BAD.name, False, 1996),
    ]
    input_df = spark.createDataFrame(input_data)
    input_df.show()
    input_df.printSchema()

    # act
    actual_df = generate_actors_history_scd(spark, input_df)
    actual_df.show()
    actual_df.printSchema()

    expected_data = [
        ActorHistoryScd("Steven Segal", QualityClass.BAD.name, True, 1995, 1995, 2020),
        ActorHistoryScd("Steven Segal", QualityClass.BAD.name, False, 1996, 1996, 2020),
        ActorHistoryScd("Tom Hanks", QualityClass.STAR.name, True, 1994, 1994, 2020),
        ActorHistoryScd("Tom Hanks", QualityClass.GOOD.name, False, 1997, 1997, 2020),
        ActorHistoryScd("Tom Hanks", QualityClass.STAR.name, True, 2000, 2003, 2020),
    ]

    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True) 