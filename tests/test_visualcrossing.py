import pandas as pd
from config import get_config
from datetime import datetime, timedelta, time, UTC
from visualcrossing_api import VisualCrossingAPI
import pytest

config = get_config()
visualcrossing_config = config.get("VisualCrossing", {})
location_config = config.get("Location", {})
API_KEY = visualcrossing_config.get("api_key")
LATITUDE = location_config.get("latitude")
LONGITUDE = location_config.get("longitude")

visual_crossing_api = VisualCrossingAPI(API_KEY)


def test_visualcrossing_forecast_multi_location():
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    # Align 'now' to the previous 15-minute boundary
    minute = (now.minute // 15) * 15
    aligned_now = now.replace(minute=minute)
    hours = 24
    end = aligned_now + timedelta(hours=hours)
    start_dt_str = aligned_now.strftime("%Y-%m-%dT%H:%M:%S")
    end_dt_str = end.strftime("%Y-%m-%dT%H:%M:%S")

    # Multi-location usage (for test, just duplicate the same location)
    multi_locations = [(LATITUDE, LONGITUDE), (LATITUDE, LONGITUDE)]
    try:
        dfs_forecast = visual_crossing_api.get_forecast(
            locations=multi_locations,
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
        )
        assert isinstance(
            dfs_forecast, list
        ), "get_forecast should return a list of DataFrames for multi-location input"
        assert len(dfs_forecast) == len(
            multi_locations
        ), "Should return one DataFrame per location"
        for idx, ((lat, lon), df_forecast) in enumerate(
            zip(multi_locations, dfs_forecast)
        ):
            print(
                f"Forecast DataFrame for location {idx} ({lat}, {lon}) head:\n{df_forecast.head()}"
            )
            assert (
                "datetime" in df_forecast.columns
            ), f"DataFrame for location {idx} should have a 'datetime' column"
            if not df_forecast.empty:
                assert pd.api.types.is_datetime64_any_dtype(
                    df_forecast["datetime"]
                ), f"'datetime' column should be of datetime type for location {idx}"
            else:
                print(f"Warning: DataFrame for location {idx} is empty.")
    except Exception as e:
        print(e)
        assert False, f"Error during interval validation: {e}"


def test_visualcrossing_forecast_single_location():
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    # Align 'now' to the previous 15-minute boundary
    minute = (now.minute // 15) * 15
    aligned_now = now.replace(minute=minute)
    hours = 24
    end = aligned_now + timedelta(hours=hours)
    start_dt_str = aligned_now.strftime("%Y-%m-%dT%H:%M:%S")
    end_dt_str = end.strftime("%Y-%m-%dT%H:%M:%S")

    # Single-location usage
    try:
        df_single = visual_crossing_api.get_forecast(
            locations=[(LATITUDE, LONGITUDE)],
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
        )
        assert isinstance(
            df_single, pd.DataFrame
        ), "get_forecast should return a DataFrame for single-location input"
        assert (
            "datetime" in df_single.columns
        ), "Single-location DataFrame should have a 'datetime' column"
        if not df_single.empty:
            assert pd.api.types.is_datetime64_any_dtype(
                df_single["datetime"]
            ), "'datetime' column should be of datetime type for single-location"
        else:
            print("Warning: Single-location DataFrame is empty.")
    except Exception as e:
        print(e)
        assert False, f"Error during interval validation: {e}"
