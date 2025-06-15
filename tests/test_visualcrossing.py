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
    thirty_mins_ago = now - timedelta(minutes=30)
    end_dt_str = now.strftime("%Y-%m-%dT%H:%M:%S")
    start_dt_str = thirty_mins_ago.strftime("%Y-%m-%dT%H:%M:%S")

    # Calculate expected number of 15-min intervals
    time_difference = now - thirty_mins_ago
    expected_intervals = int(time_difference.total_seconds() / (15 * 60))

    # Assert that the number of data points matches the expected number of intervals
    try:
        data = visual_crossing_api.get_forecast(
            latitude=LATITUDE,
            longitude=LONGITUDE,
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
            aggregate_minutes=15,
        )
        if "hours" in data:
            actual_intervals = len(data["hours"])
        elif "days" in data and data["days"]:
            actual_intervals = sum(len(day["hours"]) for day in data["days"])
        else:
            actual_intervals = 0

        assert (
            actual_intervals == expected_intervals
        ), f"Expected {expected_intervals} intervals, but got {actual_intervals} intervals"

    except Exception as e:
        print(e)
        assert False, f"Error during interval validation: {e}"
