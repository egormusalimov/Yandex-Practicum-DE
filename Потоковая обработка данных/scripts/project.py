import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, current_timestamp, unix_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
import psycopg2
import configparser

dir_path = os.path.dirname(os.path.abspath(__file__))
config_file = os.path.join(dir_path, 'project_config.ini')

config = configparser.ConfigParser()
config.read(config_file)

kafka_bootstrap_servers = config.get('kafka', 'bootstrap_servers')
kafka_security_protocol = config.get('kafka', 'security_protocol')
kafka_sasl_mechanism = config.get('kafka', 'sasl_mechanism')
kafka_sasl_jaas_config = config.get('kafka', 'sasl_jaas_config')
kafka_ssl_truststore_location = config.get('kafka', 'ssl_truststore_location')
kafka_ssl_truststore_password = config.get('kafka', 'ssl_truststore_password')

kafka_options = {
    'kafka.bootstrap.servers': kafka_bootstrap_servers,
    'kafka.security.protocol': kafka_security_protocol,
    'kafka.sasl.jaas.config': kafka_sasl_jaas_config,
    'kafka.sasl.mechanism': kafka_sasl_mechanism,
    'kafka.ssl.truststore.location': kafka_ssl_truststore_location,
    'kafka.ssl.truststore.password': kafka_ssl_truststore_password
}


def spark_init():
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )
    spark = SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
    return spark


def read_from_kafka(spark):
    restaurant_read_stream_df = spark.readStream \
        .format('kafka') \
        .options(**kafka_options) \
        .option('subscribe', 'egormusali_in') \
        .load()
    return restaurant_read_stream_df


def write_to_kafka(df):
    out_mess = df.withColumn('value',
                             to_json(struct(
                                 col('restaurant_id'),
                                 col('adv_campaign_id'),
                                 col('adv_campaign_content'),
                                 col('adv_campaign_owner'),
                                 col('adv_campaign_owner_contact'),
                                 col('adv_campaign_datetime_start'),
                                 col('adv_campaign_datetime_end'),
                                 col('datetime_created'),
                                 col('client_id'),
                                 col('trigger_datetime_created'))
                             )
                             ).select('value')
    out_mess.write \
        .format('kafka') \
        .options(**kafka_options) \
        .option('topic', 'egormusali_out') \
        .option("checkpointLocation", "query") \
        .option("truncate", False) \
        .save()


def deserialize_df(df):
    incomming_message_schema = StructType([
        StructField("restaurant_id", StringType(), nullable=True),
        StructField("adv_campaign_id", StringType(), nullable=True),
        StructField("adv_campaign_content", StringType(), nullable=True),
        StructField("adv_campaign_owner", StringType(), nullable=True),
        StructField("adv_campaign_owner_contact", StringType(), nullable=True),
        StructField("adv_campaign_datetime_start", LongType(), nullable=True),
        StructField("adv_campaign_datetime_end", LongType(), nullable=True),
        StructField("datetime_created", LongType(), nullable=True),
    ])
    filtered_read_stream_df = df.select(col("value").cast(StringType()).alias("value_str")) \
        .withColumn("deserialized_value", from_json(col("value_str"), schema=incomming_message_schema)) \
        .select("deserialized_value.*") \
        .withColumn("current_timestamp_utc", unix_timestamp(current_timestamp())) \
        .filter(
            (col("adv_campaign_datetime_start") <= col("current_timestamp_utc")) &
            (col("adv_campaign_datetime_end") >= col("current_timestamp_utc"))
        )

    return filtered_read_stream_df


# вычитываем всех пользователей с подпиской на рестораны
def read_from_postgresql(spark):
    subscribers_restaurant_df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'subscribers_restaurants') \
        .option('user', 'student') \
        .option('password', 'de-student') \
        .load()
    return subscribers_restaurant_df


def create_subscribers_feedback_table():
    connection = psycopg2.connect(f"host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.subscribers_feedback (
          id serial4 NOT NULL,
            restaurant_id text NOT NULL,
            adv_campaign_id text NOT NULL,
            adv_campaign_content text NOT NULL,
            adv_campaign_owner text NOT NULL,
            adv_campaign_owner_contact text NOT NULL,
            adv_campaign_datetime_start int8 NOT NULL,
            adv_campaign_datetime_end int8 NOT NULL,
            datetime_created int8 NOT NULL,
            client_id text NOT NULL,
            trigger_datetime_created int4 NOT NULL,
            feedback varchar NULL,
            CONSTRAINT id_pk PRIMARY KEY (id)
        );
    """)
    connection.commit()


def write_to_postgresql(df):
    try:
        connection = psycopg2.connect(f"host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
        cursor = connection.cursor()

        df.withColumn("feedback", lit(None).cast(StringType())) \
            .write.format("jdbc") \
            .mode('append') \
            .option('url', 'jdbc:postgresql://localhost:5432/de') \
            .option('dbtable', 'public.subscribers_feedback') \
            .option('user', 'jovyan') \
            .option('password', 'jovyan') \
            .option('driver', 'org.postgresql.Driver') \
            .save()
        cursor.close()
        connection.close()
    except psycopg2.Error as e:
        print("Ошибка при подключении к PostgreSQL:", e)


# метод для записи данных в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    create_subscribers_feedback_table()
    write_to_postgresql(df)
    # создаём df для отправки в Kafka. Сериализация в json.
    write_to_kafka(df)
    # очищаем память от df
    df.unpersist()


def start_streaming(df):
    df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .start() \
        .awaitTermination()


spark = spark_init()
restaurant_read_stream_df = read_from_kafka(spark)
filtered_read_stream_df = deserialize_df(restaurant_read_stream_df)
subscribers_restaurant_df = read_from_postgresql(spark)
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

filtered_read_stream_df = filtered_read_stream_df.withColumn(
    "datetime_created",
    to_timestamp(col("datetime_created"))
)

# объединяем данные из Kafka с пользователями подписки по restaurant_id + время создания события.
result_df = filtered_read_stream_df.join(subscribers_restaurant_df, 'restaurant_id') \
    .withColumn("trigger_datetime_created", unix_timestamp(current_timestamp())) \
    .withWatermark("datetime_created", "12 hours") \
    .drop('id') \
    .dropDuplicates(['client_id', 'restaurant_id'])

result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()