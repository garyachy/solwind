import pandas as pd
from config import load_config
from datetime import datetime, timedelta, time # Add this import if not already present
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

        assert (
            "timestamp" in df_historical.columns
        ), "DataFrame should have a 'timestamp' column"
        if not df_historical.empty:
            assert pd.api.types.is_datetime64_any_dtype(
                df_historical["timestamp"]
            ), "'timestamp' column should be of datetime type"

        assert (
            actual_intervals == expected_intervals
        ), f"Expected {expected_intervals} historical intervals, but got {actual_intervals}. API response (DataFrame head): {df_historical.head()}"

    except Exception as e:
        print(f"Error during historical data retrieval or validation: {e}")
        # If the API returns an error (e.g. "No data found for query"), df_historical might be empty or an error raised before.
        # Depending on expected behavior for "no data", this assertion might need adjustment.
        # For now, any exception during the API call or processing is a test failure.
        assert False, f"Error during historical data test: {e}"


def test_compare_yesterday_wind_data():
    """
    Compares historical wind data from yesterday with data fetched 
    via the 'get_forecast' method for the same period.
    Prints the differences in wind speed and wind direction.
    """
    try:
        # 1. Define yesterday
        yesterday_date_obj = (datetime.utcnow() - timedelta(days=1)).date()
        
        # For historical data API (YYYY-MM-DD)
        yesterday_date_str = yesterday_date_obj.strftime("%Y-%m-%d")

        # For forecast data API (YYYY-MM-DDTHH:MM:SS) - covering the full 24 hours of yesterday
        start_datetime_obj = datetime.combine(yesterday_date_obj, time(0, 0, 0))
        end_datetime_obj = datetime.combine(yesterday_date_obj, time(23, 59, 59)) 
        
        start_dt_str = start_datetime_obj.strftime("%Y-%m-%dT%H:%M:%S")
        end_dt_str = end_datetime_obj.strftime("%Y-%m-%dT%H:%M:%S")

        expected_intervals = 24
        wind_columns = ['windspeed', 'winddir'] # Assumed column names

        # 2. Fetch Historical Data for yesterday
        print(f"\nFetching historical data for {yesterday_date_str}")
        df_historical = visual_crossing_api.get_historical_data(
            latitude=LATITUDE,
            longitude=LONGITUDE,
            start_date=yesterday_date_str,
            end_date=yesterday_date_str,
        )
        
        assert not df_historical.empty, f"Historical data is empty for {yesterday_date_str}."
        assert "timestamp" in df_historical.columns, "Historical DataFrame missing 'timestamp' column."
        assert pd.api.types.is_datetime64_any_dtype(df_historical["timestamp"]), \
            "Historical 'timestamp' column is not datetime type."
        for col in wind_columns:
            assert col in df_historical.columns, f"Historical DataFrame missing '{col}' column."
        actual_hist_intervals = len(df_historical)
        assert actual_hist_intervals == expected_intervals, \
            f"Expected {expected_intervals} historical intervals, got {actual_hist_intervals} for {yesterday_date_str}."

        # 3. Fetch "Forecast" Data for yesterday
        print(f"Fetching 'forecast' data for yesterday: {start_dt_str} to {end_dt_str}")
        df_forecast = visual_crossing_api.get_forecast(
            latitude=LATITUDE,
            longitude=LONGITUDE,
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
        )

        assert not df_forecast.empty, f"Forecast data is empty for {start_dt_str} to {end_dt_str}."
        assert "timestamp" in df_forecast.columns, "Forecast DataFrame missing 'timestamp' column."
        if not df_forecast.empty:
             assert pd.api.types.is_datetime64_any_dtype(df_forecast["timestamp"]), \
                "Forecast 'timestamp' column is not datetime type."
        for col in wind_columns:
            assert col in df_forecast.columns, f"Forecast DataFrame missing '{col}' column."
        actual_fcst_intervals = len(df_forecast)
        assert actual_fcst_intervals == expected_intervals, \
            f"Expected {expected_intervals} forecast intervals, got {actual_fcst_intervals} for {start_dt_str} to {end_dt_str}."

        # 4. Align and Merge DataFrames
        df_historical_subset = df_historical[['timestamp'] + wind_columns].copy()
        df_forecast_subset = df_forecast[['timestamp'] + wind_columns].copy()

        df_historical_subset.sort_values('timestamp', inplace=True)
        df_forecast_subset.sort_values('timestamp', inplace=True)
        
        merged_df = pd.merge(
            df_historical_subset,
            df_forecast_subset,
            on='timestamp',
            suffixes=('_hist', '_fcst')
        )

        assert not merged_df.empty, \
            f"Merged DataFrame is empty. Timestamps might not align. Hist head:\n{df_historical_subset.head()}\nFcst head:\n{df_forecast_subset.head()}"
        assert len(merged_df) == expected_intervals, \
            f"Merged DataFrame has {len(merged_df)} rows, expected {expected_intervals}. Some timestamps might not have matched."

        # 5. Calculate Differences
        merged_df['windspeed_diff'] = merged_df['windspeed_fcst'] - merged_df['windspeed_hist']
        
        # Wind direction difference (shortest angle between -180 and 180)
        dir_diff = merged_df['winddir_fcst'] - merged_df['winddir_hist']
        merged_df['winddir_diff'] = (dir_diff + 180) % 360 - 180

        # 6. Print Differences
        print("\nComparison of Yesterday's 'Forecast' vs Historical Wind Data:")
        print("Note: 'Forecast' for a past date from Visual Crossing Timeline API typically returns historical data.")
        print("Differences should ideally be near zero if both API calls fetch the same underlying historical records.")
        
        columns_to_print = [
            'timestamp', 
            'windspeed_hist', 'windspeed_fcst', 'windspeed_diff',
            'winddir_hist', 'winddir_fcst', 'winddir_diff'
        ]
        print(merged_df[columns_to_print].to_string())

    except Exception as e:
        print(f"Error during wind data comparison test: {e}")
        assert False, f"Error during wind data comparison test: {e}"
