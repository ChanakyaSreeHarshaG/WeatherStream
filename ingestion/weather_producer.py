import json
import time
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# -----------------------------
# Configuration
# -----------------------------
API_URL = "https://api.open-meteo.com/v1/forecast"

LATITUDE = 12.9716
LONGITUDE = 77.5946
LOCATION = "Bengaluru"
TIMEZONE = "Asia/Kolkata"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "weather.forecasts"

POLL_INTERVAL_SECONDS = 900  # Fetch every 15 minutes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# -----------------------------
# Create Kafka producer
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5,
)

# -----------------------------
# Fetch weather forecast
# -----------------------------
def fetch_weather():
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
            "weather_code",
        ],
        "forecast_days": 2,
        "timezone": TIMEZONE,
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()

    if "hourly" not in data:
        raise ValueError("API response does not contain hourly forecast data")

    return data


# -----------------------------
# Transform and publish records
# -----------------------------
def publish_forecast(data):
    hourly = data["hourly"]

    # Validate that the required hourly fields are present.
    required_fields = [
        "time",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "weather_code",
    ]

    for field in required_fields:
        if field not in hourly:
            raise ValueError(f"Missing hourly field: {field}")

    # Ensure every field has a corresponding timestamp.
    forecast_count = len(hourly["time"])

    for field in required_fields[1:]:
        if len(hourly[field]) != forecast_count:
            raise ValueError(
                f"Forecast length mismatch for field: {field}"
            )

    snapshot_time = datetime.now(timezone.utc).isoformat()

    # Current Bengaluru time, rounded down to the hour.
    local_now = datetime.now(ZoneInfo(TIMEZONE))
    current_hour = local_now.replace(
        minute=0,
        second=0,
        microsecond=0,
    )

    # Convert the current hour to UTC for comparison.
    current_hour_utc = current_hour.astimezone(timezone.utc)

    published_count = 0

    for i, forecast_time in enumerate(hourly["time"]):
        # Open-Meteo returns local timestamps because timezone is specified.
        forecast_datetime = datetime.fromisoformat(
            forecast_time.replace("Z", "+00:00")
        )

        # Local timestamps without an offset belong to Bengaluru time.
        if forecast_datetime.tzinfo is None:
            forecast_datetime = forecast_datetime.replace(
                tzinfo=ZoneInfo(TIMEZONE)
            )

        # Convert to UTC so both timestamps use the same timezone.
        forecast_datetime_utc = forecast_datetime.astimezone(
            timezone.utc
        )

        # Skip forecast hours that are already in the past.
        if forecast_datetime_utc < current_hour_utc:
            continue

        # Publish no more than 24 forecast records per API fetch.
        if published_count >= 24:
            break

        record = {
            "location": LOCATION,
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "snapshot_time": snapshot_time,
            "forecast_time": forecast_datetime_utc.isoformat(),
            "temperature_c": hourly["temperature_2m"][i],
            "humidity_pct": hourly["relative_humidity_2m"][i],
            "precipitation_mm": hourly["precipitation"][i],
            "wind_speed_kmh": hourly["wind_speed_10m"][i],
            "weather_code": hourly["weather_code"][i],
        }

        try:
            # Wait for Kafka to acknowledge each record.
            future = producer.send(
                KAFKA_TOPIC,
                value=record,
            )

            metadata = future.get(timeout=30)
            published_count += 1

            logging.info(
                "Published record %d | topic=%s | partition=%s | "
                "offset=%s | forecast_time=%s",
                published_count,
                metadata.topic,
                metadata.partition,
                metadata.offset,
                record["forecast_time"],
            )

        except KafkaError:
            logging.exception(
                "Failed to publish forecast at %s",
                record["forecast_time"],
            )
            raise

    # Ensure all queued messages are delivered.
    producer.flush()

    logging.info(
        "Published %d forecast records for %s",
        published_count,
        LOCATION,
    )

    if published_count == 0:
        logging.warning(
            "No forecast records published. "
            "Check API timestamps and current UTC time."
        )


# -----------------------------
# Continuous ingestion loop
# -----------------------------
def main():
    logging.info("WeatherStream producer started.")
    logging.info("Kafka topic: %s", KAFKA_TOPIC)
    logging.info(
        "Polling interval: %d seconds",
        POLL_INTERVAL_SECONDS,
    )

    try:
        while True:
            try:
                data = fetch_weather()
                publish_forecast(data)

            except (
                requests.RequestException,
                KeyError,
                ValueError,
                KafkaError,
            ):
                logging.exception("Weather ingestion failed")

            logging.info(
                "Waiting %d seconds before the next API fetch.",
                POLL_INTERVAL_SECONDS,
            )

            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logging.info("Stopping WeatherStream producer.")

    finally:
        producer.flush()
        producer.close()
        logging.info("Kafka producer closed.")


if __name__ == "__main__":
    main()
