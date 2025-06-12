import requests
import pandas as pd  # Added import for pandas
from config import load_config

# Load configuration
config = load_config()
visualcrossing_config = config.get("VisualCrossing", {})
location_config = config.get("Location", {})  # Added to load location
API_KEY = visualcrossing_config.get("api_key")
LATITUDE = location_config.get("latitude")  # Added latitude
LONGITUDE = location_config.get("longitude")  # Added longitude

BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"


def get_visualcrossing_forecast(lat, lon, section="current"):
    # section: "current", "hours", "days"
    if not API_KEY:
        raise ValueError("API_KEY not found in config.json for VisualCrossing")
    if lat is None or lon is None:  # Added check for lat/lon
        raise ValueError("Latitude or longitude not found in config.json")
    url = f"{BASE_URL}/{lat},{lon}"
    # Map section to correct Visual Crossing 'include' value
    include_map = {"current": "current", "hourly": "hours", "daily": "days"}
    include = include_map.get(section, section)
    params = {
        "key": API_KEY,
        "unitGroup": "metric",
        "include": include,
        "lang": "en",
        "contentType": "json",
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def run_vc_forecast_test(
    latitude, longitude, section, to_df=False, print_label=None
):  # Changed station to latitude, longitude
    try:
        data = get_visualcrossing_forecast(
            latitude, longitude, section
        )  # Use latitude, longitude
        # Debug print can be removed or kept based on preference
        # print(
        #     f"DEBUG: Response keys for {latitude},{longitude}: {list(data.keys())}"
        # )
        if section == "current":
            assert (
                "currentConditions" in data
            ), f"Current data not found for {latitude},{longitude}"
            result = data["currentConditions"]
        elif section == "hourly":
            assert "hours" in data, f"Hourly data not found for {latitude},{longitude}"
            result = data["hours"]
        elif section == "daily":
            assert "days" in data, f"Daily data not found for {latitude},{longitude}"
            result = data["days"]
        else:
            raise ValueError("Unknown section")
        if to_df:
            df = pd.DataFrame(result if isinstance(result, list) else [result])
            assert not df.empty, f"{section} dataframe empty for {latitude},{longitude}"
            if print_label:
                print(f"{print_label} [{latitude},{longitude}]")
                print(df)
        else:
            assert (
                result is not None
            ), f"{section} data empty for {latitude},{longitude}"
            if print_label:
                print(f"{print_label} [{latitude},{longitude}]")
                print(result)
    except Exception as e:
        print(e)
        assert False, f"Error for location {latitude},{longitude}: {e}"


def test_visualcrossing_current_forecast():  # Removed redis_db
    run_vc_forecast_test(
        latitude=LATITUDE,  # Pass LATITUDE
        longitude=LONGITUDE,  # Pass LONGITUDE
        section="current",
        to_df=False,
        print_label="Visual Crossing Current Data:",
    )


# def test_visualcrossing_hourly_forecast_full_df():  # Removed redis_db
#     run_vc_forecast_test(
#         latitude=LATITUDE,  # Pass LATITUDE
#         longitude=LONGITUDE,  # Pass LONGITUDE
#         section="hourly",
#         to_df=True,
#         print_label="Visual Crossing Full Hourly Forecast DataFrame:",
#     )


def test_visualcrossing_daily_forecast():  # Removed redis_db
    run_vc_forecast_test(
        latitude=LATITUDE,  # Pass LATITUDE
        longitude=LONGITUDE,  # Pass LONGITUDE
        section="daily",
        to_df=True,
        print_label="Visual Crossing Daily Data as DataFrame:",
    )
