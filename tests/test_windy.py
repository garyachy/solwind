import requests
import json
from config import (
    load_config,
)
import pytest


def test_windy_api_point_forecast():
    """
    Tests basic access to the Windy API point forecast
    and checks for a successful response and expected data structure.
    """
    try:
        config = load_config()
    except FileNotFoundError:
        pytest.fail(
            "Configuration file (config.json) not found. Ensure it is in the project root."
        )
    except json.JSONDecodeError:
        pytest.fail("Error decoding JSON from config file (config.json).")

    windy_config = config.get("Windy")
    location_config = config.get("Location")

    if not windy_config:
        pytest.fail("Windy configuration missing in config.json")
    if not location_config:
        pytest.fail("Location configuration missing in config.json")

    api_key = windy_config.get("api_key")
    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if not api_key:
        pytest.fail("Windy API key missing in config.json")
    if latitude is None:
        pytest.fail("Latitude missing in location configuration")
    if longitude is None:
        pytest.fail("Longitude missing in location configuration")

    WINDY_API_URL = "https://api.windy.com/api/point-forecast/v2"
    payload = {
        "lat": latitude,
        "lon": longitude,
        "model": "gfs",  # You can choose other models like 'ecmwf', 'icon', etc.
        "parameters": ["temp", "wind", "precip"],
        "levels": ["surface"],  # Explicitly specify the 'surface' level
        "key": api_key,
    }

    print(
        f"Querying Windy API for point forecast at lat:{latitude}, lon:{longitude} using model: {payload['model']} for levels: {payload['levels']}"
    )

    try:
        response = requests.post(
            WINDY_API_URL, json=payload, timeout=20
        )  # Increased timeout
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)

        data = response.json()

        assert data is not None, "API response JSON is None."

        # Check for basic structure and metadata
        assert "ts" in data, "Response missing 'ts' (timestamps)"
        assert isinstance(data["ts"], list), "'ts' is not a list."
        assert len(data["ts"]) > 0, "'ts' list is empty."


        print(
            f"Windy API point forecast access successful. Received data for {len(data['ts'])} timestamps."
        )

    except requests.exceptions.HTTPError as http_err:
        pytest.fail(f"HTTP error occurred: {http_err} - Response: {response.text}")
    except requests.exceptions.RequestException as req_err:
        pytest.fail(f"Request error occurred: {req_err}")
    except json.JSONDecodeError:
        pytest.fail(
            f"Error decoding JSON response from Windy API. Response text: {response.text}"
        )
    except Exception as e:
        pytest.fail(f"An unexpected error occurred: {e}")


# No if __name__ == "__main__": block needed for pytest
