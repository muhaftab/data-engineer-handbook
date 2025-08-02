import os
import requests
import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf, ScalarFunction


class GetLocation(ScalarFunction):
    def eval(self, ip_address):
        url = "https://api.ip2location.io"
        response = requests.get(url, params={
            'ip': ip_address,
            'key': os.environ.get("IP_CODING_KEY")
        })

        if response.status_code != 200:
            # Return empty dict if request failed
            return json.dumps({})

        data = json.loads(response.text)

        # Extract the country and state from the response
        # This might change depending on the actual response structure
        country = data.get('country_code', '')
        state = data.get('region_name', '')
        city = data.get('city_name', '')
        return json.dumps({'country': country, 'state': state, 'city': city})

get_location = udf(GetLocation(), result_type=DataTypes.STRING())

def create_event_source_kafka(t_env: StreamTableEnvironment):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}')
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";'
        );
    """
    print(ddl)
    t_env.execute_sql(ddl)
    return table_name
    
def create_processced_events_sink_postgress(t_env: StreamTableEnvironment):
    table_name = 'processed_events'
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

def create_processced_events_sink_kafka(t_env: StreamTableEnvironment):
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
            geodata VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'muaft-processed-events',
            'format' = 'json',
            'properties.bootstrap.servers' = '{kafka_bootstrapserver}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";'
       );
    """
    
    t_env.execute_sql(ddl)
    return table_name


def do_processing():
    env = StreamExecutionEnvironment.get_execution_environment() 
    env.enable_checkpointing(15*1000)
    env.set_parallelism(2)

    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env: StreamTableEnvironment = StreamTableEnvironment.create(env, env_settings)
    t_env.create_temporary_function('get_location', get_location)

    try:
        kafka_source = create_event_source_kafka(t_env)
        postgres_sink = create_processced_events_sink_postgress(t_env)
        kafka_sink = create_processced_events_sink_kafka(t_env)

        t_env.execute_sql(
            f"""
                INSERT INTO {postgres_sink}
                SELECT 
                    ip,
                    event_timestamp,
                    referrer,
                    host,
                    url,
                    get_location(ip) as geodata
                FROM {kafka_source}
            """)

        t_env.execute_sql(
            f"""
                INSERT INTO {kafka_sink}
                SELECT
                    ip,
                    event_timestamp,
                    referrer,
                    host,
                    url,
                    get_location(ip) as geodata
                FROM {kafka_source}
            """
        ).wait()
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

if __name__ == "__main__":
    do_processing()