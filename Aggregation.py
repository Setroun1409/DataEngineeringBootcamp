import os
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_postgres_sink(t_env, table_name, ddl_columns):
    """
    Create a PostgreSQL sink table in Flink.
    """
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            {ddl_columns}
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)


def create_kafka_source(t_env):
    """
    Create a Kafka source table in Flink.
    """
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "processed_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"

    source_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def process_events_with_sessions(t_env, source_table, sink_table):
    """
    Perform session-based aggregation of events grouped by IP and host.
    """
    t_env.from_path(source_table)\
        .window(
            Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("session_window")
        ).group_by(
            col("session_window"),
            col("ip"),
            col("host")
        ) \
        .select(
            col("session_window").start.alias("session_start"),
            col("session_window").end.alias("session_end"),
            col("ip"),
            col("host"),
            col("host").count.alias("num_hits")
        ) \
        .execute_insert(sink_table)


def log_aggregation():
    """
    Main function to set up the Flink environment and execute the Flink job.
    """
    try:
        # Set up the execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.enable_checkpointing(10000)
        env.set_parallelism(3)

        # Set up the table environment
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = StreamTableEnvironment.create(env, environment_settings=settings)

        # Define tables
        source_table = create_kafka_source(t_env)

        aggregated_table_name = 'processed_events_aggregated'
        aggregated_columns = """
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            num_hits BIGINT
        """
        create_postgres_sink(t_env, aggregated_table_name, aggregated_columns)

        # Process events
        process_events_with_sessions(t_env, source_table, aggregated_table_name)

        logger.info("Flink job executed successfully.")

    except Exception as e:
        logger.error("Flink job execution failed: %s", str(e))


if __name__ == '__main__':
    log_aggregation()
