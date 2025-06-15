import pandas as pd
from config import load_config
from datetime import datetime, timedelta
from visualcrossing_api import VisualCrossingAPI

config = load_config()
visualcrossing_config = config.get("VisualCrossing", {})
location_config = config.get("Location", {})
API_KEY = visualcrossing_config.get("api_key")
LATITUDE = location_config.get("latitude")
LONGITUDE = location_config.get("longitude")

visual_crossing_api = VisualCrossingAPI(API_KEY)


def test_visualcrossing_forecast():
    now = datetime.utcnow()
    forty_eight_hours_later = now + timedelta(hours=12)
    start_dt_str = now.strftime("%Y-%m-%dT%H:%M:%S")
    end_dt_str = forty_eight_hours_later.strftime("%Y-%m-%dT%H:%M:%S")

    expected_intervals = 12

    try:
        df_forecast = visual_crossing_api.get_forecast(
            latitude=LATITUDE,
            longitude=LONGITUDE,
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
        )
        actual_intervals = len(df_forecast)

        print(f"Forecast DataFrame head:\n{df_forecast.head()}")

        assert (
            actual_intervals == expected_intervals
        ), f"Expected {expected_intervals} intervals, but got {actual_intervals} intervals. API response (DataFrame head): {df_forecast.head()}"

    except Exception as e:
        print(e)
        assert False, f"Error during interval validation: {e}"
