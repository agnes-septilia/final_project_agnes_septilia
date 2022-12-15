# import libraries
import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text


# connection to postgres
url = 'postgresql://postgres:1234@localhost:5432/postgres'
engine = create_engine(url)

# create table in postgres
with engine.connect() as conn:
    conn.execute(text("""
            CREATE TABLE IF NOT EXISTS final_project.topic_currency (
                currency_id varchar,
                currency_name varchar,
                rate float,
                timestamp varchar);
            """))

# connect to Kafka Consumer 
consumer = KafkaConsumer(
                'TopicCurrency'
                , bootstrap_servers=['localhost:9092']
                , api_version=(0,10)
                , value_deserializer = lambda m: json.loads(m.decode("utf-8"))
            )


# append each extracted data as the new row to the table in Postgres
for message in consumer:

    json_data = message.value

    event = {
        "currency_id": json_data["currency_id"],
        "currency_name": json_data["currency_name"],
        "rate": json_data["rate"],
        "timestamp": json_data["timestamp"]
    }

    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO final_project.topic_currency 
                VALUES (:currency_id, :currency_name, :rate, :timestamp)"""),
            [event]
        )
