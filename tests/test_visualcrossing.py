import pandas as pd
from config import load_config
from datetime import datetime, timedelta
from visualcrossing_api import VisualCrossingAPI

# Load configuration
config = load_config()
visualcrossing_config = config.get("VisualCrossing", {})
location_config = config.get("Location", {})
API_KEY = visualcrossing_config.get("api_key")
LATITUDE = location_config.get("latitude")
LONGITUDE = location_config.get("longitude")

# Initialize VisualCrossingAPI
visual_crossing_api = VisualCrossingAPI(API_KEY)


def test_visualcrossing_forecast():
    now = datetime.utcnow()
    forty_eight_hours_later = now + timedelta(hours=48)  # Changed to 48 hours
    start_dt_str = now.strftime("%Y-%m-%dT%H:%M:%S")
    end_dt_str = forty_eight_hours_later.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )  # Changed to 48 hours

    # Calculate expected number of 60-min (hourly) intervals
    expected_intervals = 48  # 48 hours = 48 hourly intervals # Changed to 48

    # Assert that the number of data points matches the expected number of intervals
    try:
        data = visual_crossing_api.get_forecast(
            latitude=LATITUDE,
            longitude=LONGITUDE,
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
        )
        if "hours" in data:  # This case might not be hit if data spans multiple days
            actual_intervals = len(data["hours"])
        elif "days" in data and data["days"]:
            # For a forecast starting 'now', the API might return data structured
            # across multiple 'days' if the period crosses midnight UTC.
            actual_intervals = sum(
                len(day["hours"]) for day in data["days"] if "hours" in day
            )
        else:
            actual_intervals = 0

        assert (
            actual_intervals == expected_intervals
        ), f"Expected {expected_intervals} intervals, but got {actual_intervals} intervals. API response: {data}"

    except Exception as e:
        print(e)
        assert False, f"Error during interval validation: {e}"
