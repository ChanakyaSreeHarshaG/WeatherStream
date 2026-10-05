
import requests

API_URL = "https://api.open-meteo.com/v1/forecast"

params = {
    "latitude": 12.9716,
    "longitude": 77.5946,
    "hourly": (
        "temperature_2m,"
        "relative_humidity_2m,"
        "precipitation,"
        "precipitation_probability,"
        "wind_speed_10m,"
        "wind_direction_10m,"
        "weather_code"
    ),
    "forecast_days": 1,
    "timezone": "Asia/Kolkata",
}

try:
    response = requests.get(
        API_URL,
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    hourly = data["hourly"]

    print("Weather API connection successful!")
    print("Location: Bengaluru")
    print("Timezone:", data["timezone"])
    print("\nFirst five forecast records:")

    for i in range(min(5, len(hourly["time"]))):
        record = {
            "time": hourly["time"][i],
            "temperature_c": hourly["temperature_2m"][i],
            "humidity_pct": hourly["relative_humidity_2m"][i],
            "precipitation_mm": hourly["precipitation"][i],
            "wind_speed_kmh": hourly["wind_speed_10m"][i],
            "weather_code": hourly["weather_code"][i],
        }
        print(record)

except requests.RequestException as error:
    print("API request failed:", error)

except (KeyError, ValueError) as error:
    print("Unexpected API response:", error)
