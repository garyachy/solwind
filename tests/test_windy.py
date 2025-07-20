import requests
import json
from config import get_config
import pytest
import pandas as pd  # Add this import
from datetime import datetime  # Add this import


def test_windy_api_point_forecast():
    """
    Tests basic access to the Windy API point forecast
    and checks for a successful response and expected data structure.
    """
    try:
        config = get_config()
    except Exception as e:
        pytest.fail(f"Error loading config: {e}")

    windy_config = config.get("Windy")
    location_config = config.get("Location")

    if not windy_config:
        pytest.fail("Windy configuration missing in config")
    if not location_config:
        pytest.fail("Location configuration missing in config")

    api_key = windy_config.get("api_key")
    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if not api_key:
        pytest.fail("Windy API key missing in config")
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

        # Convert timestamps to datetime objects
        # Windy API timestamps are in milliseconds
        datetimes = [datetime.fromtimestamp(ts / 1000) for ts in data["ts"]]

        # Create a dictionary for DataFrame creation
        # Initialize with datetime
        df_data = {"datetime": datetimes}

        # Add other parameters to the dictionary
        # Parameters are typically named like 'param-level', e.g., 'temp-surface'
        for param in payload["parameters"]:
            for level in payload[
                "levels"
            ]:  # Or however levels are structured in response keys
                # Construct the key as it appears in the API response
                # This might need adjustment based on the exact response format
                # e.g., 'temp-surface', 'wind_u-surface'
                # For simplicity, let's assume keys are like 'temp-surface'
                # You might need to inspect `data.keys()` to get the exact names
                response_param_key_found = False
                for key_suffix_variant in [
                    f"-{level}",
                    f"_{level}",
                ]:  # try common variants
                    potential_key = f"{param}{key_suffix_variant}"
                    if potential_key in data:
                        assert isinstance(
                            data[potential_key], list
                        ), f"'{potential_key}' is not a list."
                        assert len(data[potential_key]) == len(
                            data["ts"]
                        ), f"'{potential_key}' length mismatch with 'ts'."
                        df_data[potential_key] = data[potential_key]
                        response_param_key_found = True
                        break
                if not response_param_key_found:
                    # Fallback if level is not in the key name or a different convention is used
                    if (
                        param in data
                        and isinstance(data[param], list)
                        and len(data[param]) == len(data["ts"])
                    ):
                        df_data[param] = data[param]
                    else:
                        print(
                            f"Warning: Parameter '{param}' for level '{level}' (or base '{param}') not found or mismatched in response data keys: {list(data.keys())}"
                        )

        # Create Pandas DataFrame
        df = pd.DataFrame(df_data)

        # Ensure 'datetime' is the first column (it should be by construction, but good to be explicit)
        if "datetime" in df.columns:
            cols = ["datetime"] + [col for col in df.columns if col != "datetime"]
            df = df[cols]

        assert not df.empty, "Pandas DataFrame is empty."
        assert "datetime" in df.columns, "DataFrame missing 'datetime' column."
        assert pd.api.types.is_datetime64_any_dtype(
            df["datetime"]
        ), "'datetime' column is not of datetime type."

        print(
            f"Windy API point forecast access successful. Received data for {len(data['ts'])} timestamps."
        )
        print("DataFrame created successfully:")
        print(df.head())

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
