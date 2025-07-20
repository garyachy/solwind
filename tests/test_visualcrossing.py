import pandas as pd
from config import load_config
from datetime import datetime, timedelta, time
from visualcrossing_api import VisualCrossingAPI
import pytest

config = load_config()
visualcrossing_config = config.get("VisualCrossing", {})
location_config = config.get("Location", {})
API_KEY = visualcrossing_config.get("api_key")
LATITUDE = location_config.get("latitude")
LONGITUDE = location_config.get("longitude")

visual_crossing_api = VisualCrossingAPI(API_KEY)


def test_visualcrossing_forecast_multi_location():
    now = datetime.utcnow().replace(second=0, microsecond=0)
    # Align 'now' to the previous 15-minute boundary
    minute = (now.minute // 15) * 15
    aligned_now = now.replace(minute=minute)
    hours = 24
    end = aligned_now + timedelta(hours=hours)
    start_dt_str = aligned_now.strftime("%Y-%m-%dT%H:%M:%S")
    end_dt_str = end.strftime("%Y-%m-%dT%H:%M:%S")

    # Generate expected timestamps at 15-minute intervals between aligned_now and twelve_hours_later
    expected_timestamps = pd.date_range(
        start=aligned_now,
        end=end,
        freq="15min",
    )

    # Multi-location usage (for test, just duplicate the same location)
    multi_locations = [(LATITUDE, LONGITUDE), (LATITUDE, LONGITUDE)]
    try:
        dfs_forecast = visual_crossing_api.get_forecast(
            locations=multi_locations,
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
        )
        assert isinstance(dfs_forecast, list), "get_forecast should return a list of DataFrames for multi-location input"
        assert len(dfs_forecast) == len(multi_locations), "Should return one DataFrame per location"
        for idx, ((lat, lon), df_forecast) in enumerate(zip(multi_locations, dfs_forecast)):
            print(f"Forecast DataFrame for location {idx} ({lat}, {lon}) head:\n{df_forecast.head()}")
            assert (
                "timestamp" in df_forecast.columns
            ), f"DataFrame for location {idx} should have a 'timestamp' column"
            if not df_forecast.empty:
                assert pd.api.types.is_datetime64_any_dtype(
                    df_forecast["timestamp"]
                ), f"'timestamp' column should be of datetime type for location {idx}"
                timestamps = (
                    pd.Series(df_forecast["timestamp"]).sort_values().reset_index(drop=True)
                )
                expected_range = pd.date_range(
                    start=timestamps.iloc[0],
                    end=timestamps.iloc[-1],
                    freq="15min",
                )
                assert list(timestamps) == list(expected_range), (
                    f"Timestamps do not form a continuous 15-minute interval sequence for location ({lat}, {lon}).\n"
                    f"Expected: {list(expected_range)}\n"
                    f"Got: {list(timestamps)}"
                )
                timestamps_set = set(timestamps)
                missing = [ts for ts in expected_timestamps if ts not in timestamps_set]
                assert not missing, (
                    f"Missing expected 15-min timestamps between aligned_now and twelve_hours_later for location ({lat}, {lon}): {missing}\n"
                    f"Returned timestamps: {list(timestamps)}"
                )
            else:
                print(f"Warning: DataFrame for location {idx} is empty.")
    except Exception as e:
        print(e)
        assert False, f"Error during interval validation: {e}"


def test_visualcrossing_forecast_single_location():
    now = datetime.utcnow().replace(second=0, microsecond=0)
    # Align 'now' to the previous 15-minute boundary
    minute = (now.minute // 15) * 15
    aligned_now = now.replace(minute=minute)
    hours = 24
    end = aligned_now + timedelta(hours=hours)
    start_dt_str = aligned_now.strftime("%Y-%m-%dT%H:%M:%S")
    end_dt_str = end.strftime("%Y-%m-%dT%H:%M:%S")

    # Generate expected timestamps at 15-minute intervals between aligned_now and twelve_hours_later
    expected_timestamps = pd.date_range(
        start=aligned_now,
        end=end,
        freq="15min",
    )

    # Single-location usage
    try:
        df_single = visual_crossing_api.get_forecast(
            locations=[(LATITUDE, LONGITUDE)],
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
        )
        assert isinstance(df_single, pd.DataFrame), "get_forecast should return a DataFrame for single-location input"
        assert "timestamp" in df_single.columns, "Single-location DataFrame should have a 'timestamp' column"
        if not df_single.empty:
            assert pd.api.types.is_datetime64_any_dtype(df_single["timestamp"]), "'timestamp' column should be of datetime type for single-location"
            timestamps = pd.Series(df_single["timestamp"]).sort_values().reset_index(drop=True)
            expected_range = pd.date_range(
                start=timestamps.iloc[0],
                end=timestamps.iloc[-1],
                freq="15min",
            )
            assert list(timestamps) == list(expected_range), (
                f"Timestamps do not form a continuous 15-minute interval sequence for single-location.\n"
                f"Expected: {list(expected_range)}\n"
                f"Got: {list(timestamps)}"
            )
            timestamps_set = set(timestamps)
            missing = [ts for ts in expected_timestamps if ts not in timestamps_set]
            assert not missing, (
                f"Missing expected 15-min timestamps between aligned_now and twelve_hours_later for single-location: {missing}\n"
                f"Returned timestamps: {list(timestamps)}"
            )
        else:
            print("Warning: Single-location DataFrame is empty.")
    except Exception as e:
        print(e)
        assert False, f"Error during interval validation: {e}"


@pytest.mark.skip(reason="Temporarily disabled for investigation or maintenance.")
def test_visualcrossing_historical_data():
    # Define a date range for historical data, e.g., 1 day ago to today
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=1)

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # For a 2-day period (start_date and end_date inclusive), 2 * 96 = 192 intervals
    expected_intervals = 192

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
        assert False, f"Error during historical data test: {e}"


@pytest.mark.skip(reason="Temporarily disabled for investigation or maintenance.")
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

        # For 1 day at 15-minute intervals: 96 intervals
        expected_intervals = 96
        wind_columns = ["windspeed", "winddir"]  # Assumed column names

        # 2. Fetch Historical Data for yesterday
        print(f"\nFetching historical data for {yesterday_date_str}")
        df_historical = visual_crossing_api.get_historical_data(
            latitude=LATITUDE,
            longitude=LONGITUDE,
            start_date=yesterday_date_str,
            end_date=yesterday_date_str,
        )

        assert (
            not df_historical.empty
        ), f"Historical data is empty for {yesterday_date_str}."
        assert (
            "timestamp" in df_historical.columns
        ), "Historical DataFrame missing 'timestamp' column."
        assert pd.api.types.is_datetime64_any_dtype(
            df_historical["timestamp"]
        ), "Historical 'timestamp' column is not datetime type."
        for col in wind_columns:
            assert (
                col in df_historical.columns
            ), f"Historical DataFrame missing '{col}' column."
        actual_hist_intervals = len(df_historical)
        assert (
            actual_hist_intervals == expected_intervals
        ), f"Expected {expected_intervals} historical intervals, got {actual_hist_intervals} for {yesterday_date_str}."

        # 3. Fetch "Forecast" Data for yesterday
        print(f"Fetching 'forecast' data for yesterday: {start_dt_str} to {end_dt_str}")
        df_forecast = visual_crossing_api.get_forecast(
            latitude=LATITUDE,
            longitude=LONGITUDE,
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
        )

        assert (
            not df_forecast.empty
        ), f"Forecast data is empty for {start_dt_str} to {end_dt_str}."
        assert (
            "timestamp" in df_forecast.columns
        ), "Forecast DataFrame missing 'timestamp' column."
        if not df_forecast.empty:
            assert pd.api.types.is_datetime64_any_dtype(
                df_forecast["timestamp"]
            ), "Forecast 'timestamp' column is not datetime type."
        for col in wind_columns:
            assert (
                col in df_forecast.columns
            ), f"Forecast DataFrame missing '{col}' column."
        actual_fcst_intervals = len(df_forecast)
        assert (
            actual_fcst_intervals == expected_intervals
        ), f"Expected {expected_intervals} forecast intervals, got {actual_fcst_intervals} for {start_dt_str} to {end_dt_str}."

        # 4. Align and Merge DataFrames
        df_historical_subset = pd.DataFrame(
            df_historical[["timestamp"] + wind_columns].copy()
        )
        df_forecast_subset = pd.DataFrame(
            df_forecast[["timestamp"] + wind_columns].copy()
        )

        df_historical_subset = df_historical_subset.sort_values(
            "timestamp", inplace=False
        )
        df_forecast_subset = df_forecast_subset.sort_values("timestamp", inplace=False)

        merged_df = pd.merge(
            df_historical_subset,
            df_forecast_subset,
            on="timestamp",
            suffixes=("_hist", "_fcst"),
        )

        assert (
            not merged_df.empty
        ), f"Merged DataFrame is empty. Timestamps might not align. Hist head:\n{df_historical_subset.head()}\nFcst head:\n{df_forecast_subset.head()}"
        assert (
            len(merged_df) == expected_intervals
        ), f"Merged DataFrame has {len(merged_df)} rows, expected {expected_intervals}. Some timestamps might not have matched."

        # 5. Calculate Differences
        merged_df["windspeed_diff"] = (
            merged_df["windspeed_fcst"] - merged_df["windspeed_hist"]
        )

        # Wind direction difference (shortest angle between -180 and 180)
        dir_diff = merged_df["winddir_fcst"] - merged_df["winddir_hist"]
        merged_df["winddir_diff"] = (dir_diff + 180) % 360 - 180

        # 6. Print Differences
        print("\nComparison of Yesterday's 'Forecast' vs Historical Wind Data:")
        print(
            "Note: 'Forecast' for a past date from Visual Crossing Timeline API typically returns historical data."
        )
        print(
            "Differences should ideally be near zero if both API calls fetch the same underlying historical records."
        )

        columns_to_print = [
            "timestamp",
            "windspeed_hist",
            "windspeed_fcst",
            "windspeed_diff",
            "winddir_hist",
            "winddir_fcst",
            "winddir_diff",
        ]
        print(merged_df[columns_to_print].to_string())

    except Exception as e:
        print(f"Error during wind data comparison test: {e}")
        assert False, f"Error during wind data comparison test: {e}"
