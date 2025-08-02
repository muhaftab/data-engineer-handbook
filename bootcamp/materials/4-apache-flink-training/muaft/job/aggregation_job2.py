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
    pattern = "yyyy-MM-dd HH:mm:ss.SSS"

    ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'muaft-processed-events',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}2',
            'properties.bootstrap.servers' = '{kafka_bootstrapserver}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";'
       );
    """
    t_env.execute_sql(ddl)

    return table_name

def create_sink(t_env: StreamTableEnvironment):
    table_name = 'processed_events_test'
    ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
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
        sink_table = create_sink(t_env)

        #t_env.execute_sql("SELECT * FROM processed_events_kafka").print()

        t_env.execute_sql(f"INSERT INTO {sink_table} SELECT * FROM {source_table}").wait()       
    except Exception as e:
        print("Error in flink job", e)
    
if __name__ == "__main__":
    do_processing()