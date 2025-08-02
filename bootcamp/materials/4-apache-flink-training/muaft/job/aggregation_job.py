import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.window import Tumble
from pyflink.table.expressions import lit, col


def create_processced_events_source_kafka(t_env: StreamTableEnvironment):
    kafka_bootstrapserver = os.environ.get("CONFLUENT_CLOUD_BOOTSTRAP_SERVER")
    kafka_key = os.environ.get("CONFLUENT_CLOUD_KAFKA_API_KEY")
    kafka_secret = os.environ.get("CONFLUENT_CLOUD_KAFKA_API_SECRET")
    table_name = "processed_events_kafka"

    ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            WATERMARK for event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'muaft-processed-events',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.bootstrap.servers' = '{kafka_bootstrapserver}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";'
       );
    """
    t_env.execute_sql(ddl)

    return table_name

def create_aggregated_sink(t_env: StreamTableEnvironment):
    table_name = 'processed_events_aggregated'
    ddl = f"""
        CREATE TABLE {table_name} (
            event_hour TIMESTAMP(3),
            host VARCHAR,
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(ddl)
    
    return table_name

def create_aggregated_sink_referrer(t_env: StreamTableEnvironment):
    table_name = 'processed_events_aggregated_referred'
    ddl = f"""
        CREATE TABLE {table_name} (
            event_hour TIMESTAMP(3),
            host VARCHAR,
            referrer VARCHAR,
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(ddl)
    
    return table_name


def do_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(15*1000)
    env.set_parallelism(1)

    env_setting = EnvironmentSettings.new_instance().in_streaming_mode().build() 
    t_env: StreamTableEnvironment = StreamTableEnvironment.create(env, env_setting)

    try:
        source_table = create_processced_events_source_kafka(t_env)
        aggregated_sink_table = create_aggregated_sink(t_env) 
        aggregated_sink_table_host = create_aggregated_sink_referrer(t_env) 
       
        t_env.from_path(source_table).window(
            Tumble.over(lit(30).seconds).on(col('event_timestamp')).alias('w')
        ).group_by(
            col('w'),
            col('host')
        ).select(
            col('w').start.alias('event_hour'),
            col('host'),
            col('host').count.alias('num_hits') ,
        ).execute_insert(aggregated_sink_table)
 
        t_env.from_path(source_table).window(
            Tumble.over(lit(5).minute).on(col('event_timestamp')).alias('w')
        ).group_by(
            col('w'),
            col('host'),
            col('referrer')
        ).select(
            col('w').start.alias('event_hour'),
            col('host'),
            col('referrer'),
            col('host').count.alias('num_hits')
        ).execute_insert(aggregated_sink_table_host).wait()
    except Exception as e:
        print("Error in flink job", e)
    
if __name__ == "__main__":
    do_processing()