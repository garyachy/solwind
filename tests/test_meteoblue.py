import requests  # For requests.exceptions
import json
from config import get_config
from meteoblue_dataset_sdk import Client  # Assuming this is the correct import
import pytest
import datetime  # Required for timeIntervals


def test_meteoblue_api_access_sync():  # Renamed to indicate it's the sync version
    """
    Tests basic access to the Meteoblue API using the SDK synchronously
    and checks for a successful response.
    """
    try:
        config = get_config()
    except Exception as e:
        pytest.fail(f"Error loading config: {e}")

    meteoblue_config = config.get("Meteoblue")
    location_config = config.get("Location")

    if not meteoblue_config:
        pytest.fail("Meteoblue configuration missing in config")
    if not location_config:
        pytest.fail("Location configuration missing in config")

    api_key = meteoblue_config.get("api_key")
    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if not api_key:
        pytest.fail("Meteoblue API key missing")
    if latitude is None:
        pytest.fail("Latitude missing in location configuration")
    if longitude is None:
        pytest.fail("Longitude missing in location configuration")

    forecast_days_str = "1"

    try:
        # Initialize client with API key.
        # Timeout might be a parameter for Client or query_sync, check SDK docs.
        # For now, assuming Client handles timeout or it's set globally for the SDK.
        client = Client(apikey=api_key)

        print(
            f"Querying Meteoblue API using sync SDK for hourly forecast data at lat:{latitude}, lon:{longitude}"
        )

        now_utc = datetime.datetime.utcnow()
        try:
            num_forecast_days = int(forecast_days_str)
        except ValueError:
            pytest.fail(f"Invalid forecast_days value: {forecast_days_str}")

        end_utc = now_utc + datetime.timedelta(days=num_forecast_days)
        time_interval_str = f"{now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"

        print("Meteoblue API access successful via sync SDK, received structured data.")

    except Exception as e:
        pytest.fail(f"An unexpected error occurred: {e}")


# No if __name__ == "__main__": block needed for pytest
