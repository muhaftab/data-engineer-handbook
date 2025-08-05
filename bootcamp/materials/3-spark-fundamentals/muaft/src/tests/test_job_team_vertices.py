from collections import namedtuple
from jobs.job_team_vertices import do_team_vertices_transformation
from chispa.dataframe_comparer import assert_df_equality

Team = namedtuple("Team", "team_id abbreviation nickname city arena yearfounded")
TeamVertex = namedtuple("TeamVertex", "identifier type properties")

def test_vertices_generation(spark):
    # arrnage
    input_data = [
        Team(1, "GSW", "Warriors", "San Francisco", "Chase Center", 1900),
        Team(1, "GSW", "Bad Warriors", "San Francisco", "Chase Center", 1900),
    ]
    indput_df = spark.createDataFrame(input_data)

    # act
    actual_df = do_team_vertices_transformation(spark, indput_df)

    expected_data = [TeamVertex(1, "team", {'abbreviation': 'GSW', 'nickname': 'Warriors', 'city': 'San Francisco',
                'arena': 'Chase Center',
                'year_founded': '1900'
            })]

    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True) 