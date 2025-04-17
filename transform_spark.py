from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from dotenv import load_dotenv


load_dotenv()

kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
topic_name = os.getenv('KAFKA_TOPIC_NAME')
os.environ['SPARK_USER'] = 'root'

# Создаем объект Spark Session
spark = SparkSession.builder \
    .appName("KafkaToConsole") \
    .getOrCreate()

test = spark.range(10)
test.show()

spark.stop()

# # Читаем данные из Kafka
# raw_stream = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#     .option("subscribe", topic_name) \
#     .load()

# # Преобразовываем данные в нужном формате
# orders = raw_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

