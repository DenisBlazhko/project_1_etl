import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
from confluent_kafka import Producer


load_dotenv()

# Подключаемся к PostgreSQL
conn = psycopg2.connect(
    database=os.getenv('database'),
    user=os.getenv('user'),
    password=os.getenv('password'),
    host=os.getenv('host'),
    port=os.getenv('port'))

# Извлекаем свежие данные
query = "SELECT * FROM blazhko_stg.orders WHERE created_at >= DATE_TRUNC('month', current_date)"
orders_df = pd.read_sql(query, conn)

# Закрываем соединение
conn.close()


# Настройки брокера Kafka
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
topic_name = os.getenv('KAFKA_TOPIC_NAME')

# Конфигурация производителя сообщений Kafka
producer_config = {
    'bootstrap.servers': kafka_bootstrap_servers
}

# Создаём объект-производителя сообщений
producer = Producer(producer_config)

def delivery_report(err, msg):
    """Обработчик доставки сообщений"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        pass
        #print(f'Message delivered to topic {msg.topic()} at partition [{msg.partition()}] offset {msg.offset()}')

# Отправляем каждую запись в Kafka
for index, row in orders_df.iterrows():
    # Конвертируем ряд в JSON и отправляем сообщение
    message = row.to_json().encode('utf-8')
    producer.produce(topic_name, value=message, callback=delivery_report)

# Ждем завершения отправки всех сообщений
producer.flush()

print("Все сообщения были отправлены!")