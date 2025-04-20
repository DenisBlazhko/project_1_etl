from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from dotenv import load_dotenv
import time

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

load_dotenv()

kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
topic_name = os.getenv('KAFKA_TOPIC_NAME')
#os.environ['SPARK_USER'] = 'root'

# Создаем объект Spark Session
spark = SparkSession.builder \
    .appName("KafkaToConsole") \
    .master("local") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .getOrCreate()

# test = spark.range(10)
# test.show()



# Читаем данные из Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Преобразовываем данные в нужном формате
orders = raw_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Выводим данные в консоль
query = orders \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()


spark.stop()