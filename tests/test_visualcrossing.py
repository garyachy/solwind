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


def test_visualcrossing_historical_data():
    # Define a date range for historical data, e.g., 2 days ago to 1 day ago
    end_date = datetime.utcnow() - timedelta(days=1)
    start_date = end_date - timedelta(days=1)

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # For a 1-day range (inclusive of start, exclusive of end for hours, but API might include full days)
    # Visual Crossing API typically returns hourly data for the specified days.
    # If start_date and end_date define a period of D days, we expect D * 24 hours.
    # For start_date to end_date (inclusive), it's (end_date - start_date).days + 1 days.
    # In this case, (end_date - start_date).days is 1. So, 2 days of data.
    # However, the API documentation should be the source of truth for interval counts.
    # Let's assume it returns 24 hours for each day in the range, including the end_date.
    # So for a 2-day period (start_date and end_date inclusive), we expect 48 hourly intervals.
    expected_intervals = 48  # 2 days * 24 hours/day

    try:
        df_historical = visual_crossing_api.get_historical_data(
            latitude=LATITUDE,
            longitude=LONGITUDE,
            start_date=start_date_str,
            end_date=end_date_str,
        )
        actual_intervals = len(df_historical)

        print(f"Historical DataFrame head:\n{df_historical.head()}")
        print(f"Historical DataFrame tail:\n{df_historical.tail()}")
        print(f"Number of historical intervals: {actual_intervals}")


        assert "timestamp" in df_historical.columns, "DataFrame should have a 'timestamp' column"
        if not df_historical.empty:
            assert pd.api.types.is_datetime64_any_dtype(df_historical['timestamp']), "'timestamp' column should be of datetime type"

        assert (
            actual_intervals == expected_intervals
        ), f"Expected {expected_intervals} historical intervals, but got {actual_intervals}. API response (DataFrame head): {df_historical.head()}"

    except Exception as e:
        print(f"Error during historical data retrieval or validation: {e}")
        # If the API returns an error (e.g. "No data found for query"), df_historical might be empty or an error raised before.
        # Depending on expected behavior for "no data", this assertion might need adjustment.
        # For now, any exception during the API call or processing is a test failure.
        assert False, f"Error during historical data test: {e}"
