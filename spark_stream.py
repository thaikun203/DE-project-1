import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    """Create the Cassandra keyspace."""
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")


def create_table(session):
    """Create the table in Cassandra."""
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.wikimedia_changes (
        id UUID PRIMARY KEY,
        wikimedia_change_id TEXT,
        type TEXT,
        title TEXT,
        user TEXT,
        timestamp TEXT,
        comment TEXT);
    """)
    print("Table created successfully!")


def insert_data(session, **kwargs):
    """Insert data into the Cassandra table."""
    print("Inserting data...")

    user_id = kwargs.get('id')
    wikimedia_change_id = kwargs.get('wikimedia_change_id')
    change_type = kwargs.get('type')
    title = kwargs.get('title')
    user = kwargs.get('user')
    timestamp = kwargs.get('timestamp')
    comment = kwargs.get('comment')

    try:
        session.execute("""
            INSERT INTO spark_streams.wikimedia_changes(id, wikimedia_change_id, type, title, user, 
                timestamp, comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (user_id, wikimedia_change_id, change_type, title, user, timestamp, comment))
        logging.info(f"Data inserted for change {title} by {user}")

    except Exception as e:
        logging.error(f'Could not insert data due to {e}')


def create_spark_connection():
    """Create and return a SparkSession."""
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.4") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    """Connect to Kafka and return the DataFrame."""
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    """Create and return a Cassandra session."""
    try:
        # Connecting to the Cassandra cluster
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    """Parse the Kafka stream data into the desired schema."""
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("wikimedia_change_id", StringType(), False),
        StructField("type", StringType(), False),
        StructField("title", StringType(), False),
        StructField("user", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("comment", StringType(), False)
    ])

    # Parse the incoming JSON from Kafka
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka with Spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            # Start the Spark streaming process and write data to Cassandra
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'wikimedia_changes')
                               .foreachBatch(lambda df, epoch_id: df.foreach(lambda row: insert_data(session, **row.asDict())))
                               .start())

            streaming_query.awaitTermination()
