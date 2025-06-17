import requests  # For requests.exceptions
import json
from config import load_config
from meteoblue_dataset_sdk import Client  # Assuming this is the correct import
import pytest
import datetime  # Required for timeIntervals


def test_meteoblue_api_access_sync():  # Renamed to indicate it's the sync version
    """
    Tests basic access to the Meteoblue API using the SDK synchronously
    and checks for a successful response.
    """
    try:
        config = load_config()
    except FileNotFoundError:
        pytest.fail(
            "Configuration file (config.json) not found. Ensure it is in the project root."
        )
    except json.JSONDecodeError:
        pytest.fail("Error decoding JSON from config file (config.json).")

    meteoblue_config = config.get("Meteoblue")
    location_config = config.get("Location")

    if not meteoblue_config:
        pytest.fail("Meteoblue configuration missing in config.json")
    if not location_config:
        pytest.fail("Location configuration missing in config.json")

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

        query = {
            "units": {"temperature": "C", "velocity": "km/h", "length": "metric"},
            "geometry": {
                "type": "Point",
                "coordinates": [
                    longitude,
                    latitude,
                ],  # SDK expects [longitude, latitude]
            },
            "format": "json",  # Keep as json for assertions on structured object
            "timeIntervals": [time_interval_str],
            "queries": [
                {
                    "domain": "NEMSGLOBAL",
                    "timeResolution": "hourly",
                    "codes": [
                        {"code": 11, "level": "2 m above gnd"},  # Temperature at 2m
                        {"code": 32, "level": "10 m above gnd"},  # Wind Speed at 10m
                        {"code": 71},  # Total Precipitation (hourly accumulation) - level often implicit
                    ],
                }
            ],
        }

        result = client.query_sync(query)  # Use query_sync

        assert result is not None, "API response object is None."

        # Assertions based on the example's structured result object
        assert hasattr(
            result, "geometries"
        ), "Result object missing 'geometries' attribute."
        assert isinstance(result.geometries, list), "'geometries' is not a list."
        assert len(result.geometries) > 0, "'geometries' list is empty."

        geometry_data = result.geometries[0]
        assert hasattr(
            geometry_data, "codes"
        ), "Geometry object missing 'codes' attribute."
        assert isinstance(geometry_data.codes, list), "'codes' is not a list."
        assert len(geometry_data.codes) > 0, "'codes' list is empty."

        # Check data for the first requested code (e.g., temperature)
        code_data = geometry_data.codes[0]
        assert hasattr(
            code_data, "timeIntervals"
        ), "Code object missing 'timeIntervals' attribute."
        assert isinstance(
            code_data.timeIntervals, list
        ), "'timeIntervals' is not a list."
        assert len(code_data.timeIntervals) > 0, "'timeIntervals' list is empty."

        time_interval_data = code_data.timeIntervals[0]
        assert hasattr(
            time_interval_data, "data"
        ), "TimeInterval object missing 'data' attribute."
        assert isinstance(time_interval_data.data, list), "Data for code is not a list."
        # assert len(time_interval_data.data) > 0, "Data list for code is empty." # Data could be empty

        print("Meteoblue API access successful via sync SDK, received structured data.")

    except Exception as e:
        pytest.fail(f"An unexpected error occurred: {e}")


# No if __name__ == "__main__": block needed for pytest
