import json
import logging
import os

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# -----------------------------
# Configuration
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "weather.forecasts"
KAFKA_GROUP_ID = "weatherstream-consumer-group"

# PostgreSQL connection settings
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "weatherstream",
    "user": "postgres",
    "password": os.getenv("PGPASSWORD"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def insert_weather_record(cursor, record):
    """Insert one Kafka weather message into PostgreSQL."""

    query = """
        INSERT INTO weather_forecasts (
            location,
            latitude,
            longitude,
            snapshot_time,
            forecast_time,
            temperature_c,
            humidity_pct,
            precipitation_mm,
            wind_speed_kmh,
            weather_code
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """

    values = (
        record.get("location"),
        record.get("latitude"),
        record.get("longitude"),
        record.get("snapshot_time"),
        record.get("forecast_time"),
        record.get("temperature_c"),
        record.get("humidity_pct"),
        record.get("precipitation_mm"),
        record.get("wind_speed_kmh"),
        record.get("weather_code"),
    )

    cursor.execute(query, values)


def main():
    consumer = None
    connection = None

    try:
        if not DB_CONFIG["password"]:
            raise ValueError(
                "Set the PGPASSWORD environment variable "
                "to your PostgreSQL password."
            )

        # Connect to PostgreSQL
        connection = psycopg2.connect(**DB_CONFIG)
        connection.autocommit = False

        logging.info("Connected to PostgreSQL database.")

        # Create Kafka consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda message: json.loads(
                message.decode("utf-8")
            ),
        )

        logging.info("WeatherStream consumer started.")
        logging.info("Listening to topic: %s", KAFKA_TOPIC)

        with connection.cursor() as cursor:
            for message in consumer:
                record = message.value

                try:
                    insert_weather_record(cursor, record)
                    connection.commit()

                    # Commit Kafka offset only after the database commit.
                    consumer.commit()

                    logging.info(
                        "Weather saved | Location: %s | "
                        "Forecast time: %s | Temperature: %s C",
                        record.get("location"),
                        record.get("forecast_time"),
                        record.get("temperature_c"),
                    )

                except (psycopg2.Error, TypeError, ValueError):
                    connection.rollback()
                    logging.exception(
                        "Failed to save weather record to PostgreSQL."
                    )
                    # Do not commit this Kafka offset.
                    # Stop to avoid repeatedly processing a bad message.
                    raise

    except (KafkaError, psycopg2.Error, json.JSONDecodeError, ValueError):
        logging.exception("WeatherStream consumer failed.")

    except KeyboardInterrupt:
        logging.info("Stopping WeatherStream consumer.")

    finally:
        if consumer is not None:
            consumer.close()
            logging.info("Kafka consumer closed.")

        if connection is not None:
            connection.close()
            logging.info("PostgreSQL connection closed.")


if __name__ == "__main__":
    main()