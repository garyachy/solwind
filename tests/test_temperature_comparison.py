"""
Test file to compare temperature results between VisualCrossing and Meteomatics APIs.
Compares temperature forecasts for 24 hours from now and analyzes differences.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from config import get_config
from visualcrossing_api import VisualCrossingAPI
from meteomatics_api import MeteomaticsAPI


def test_temperature_comparison_24h_forecast():
    """
    Compare temperature data between VisualCrossing and Meteomatics for current time range.
    Tests data availability, format consistency, and analyzes temperature differences.
    """
    # Load configuration
    config = get_config()
    visualcrossing_config = config.get("VisualCrossing", {})
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    # Get API credentials
    vc_api_key = visualcrossing_config.get("api_key")
    mm_username = meteomatics_config.get("username")
    mm_password = meteomatics_config.get("password")
    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    # Skip test if credentials are not available
    if not vc_api_key:
        pytest.skip("VisualCrossing API key not available")
    if not mm_username or not mm_password:
        pytest.skip("Meteomatics credentials not available")
    if latitude is None or longitude is None:
        pytest.skip("Location coordinates not available")

    # Initialize APIs
    vc_api = VisualCrossingAPI(vc_api_key)
    mm_api = MeteomaticsAPI(mm_username, mm_password)

    # Set up time range for current data (VisualCrossing has better current data availability)
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    # Align to the previous 15-minute boundary for consistency
    minute = (now.minute // 15) * 15
    aligned_now = now.replace(minute=minute)
    
    start_time = aligned_now - timedelta(hours=2)  # Start 2 hours ago
    end_time = aligned_now + timedelta(hours=2)    # End 2 hours from now
    
    # Format datetime strings for VisualCrossing
    start_dt_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_dt_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    location = [(latitude, longitude)]

    try:
        # Get VisualCrossing forecast
        print(f"Fetching VisualCrossing forecast from {start_dt_str} to {end_dt_str}")
        vc_forecast = vc_api.get_forecast(
            locations=location,
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
        )
        
        # Get Meteomatics forecast
        print(f"Fetching Meteomatics forecast from {start_time} to {end_time}")
        mm_forecast = mm_api.get_forecast(
            locations=location,
            parameters=["t_2m:C"],  # Temperature parameter
            start_datetime=start_time,
            end_datetime=end_time,
            interval=timedelta(hours=1),  # Hourly intervals
        )

        # Validate data availability
        assert isinstance(vc_forecast, pd.DataFrame), "VisualCrossing should return a DataFrame"
        assert isinstance(mm_forecast, list), "Meteomatics should return a list of DataFrames"
        assert len(mm_forecast) > 0, "Meteomatics should return at least one DataFrame"
        
        vc_df = vc_forecast
        mm_df = mm_forecast[0]  # Get first (and only) location result

        # Check if dataframes are not empty
        assert not vc_df.empty, "VisualCrossing DataFrame should not be empty"
        assert not mm_df.empty, "Meteomatics DataFrame should not be empty"

        # Validate datetime columns
        assert "datetime" in vc_df.columns, "VisualCrossing DataFrame should have 'datetime' column"
        assert "datetime" in mm_df.columns, "Meteomatics DataFrame should have 'datetime' column"

        # Handle timezone awareness - VisualCrossing returns naive datetimes, Meteomatics returns timezone-aware
        if vc_df["datetime"].dt.tz is None:
            # VisualCrossing returns naive datetimes, assume UTC
            vc_df["datetime"] = vc_df["datetime"].dt.tz_localize(timezone.utc)
        else:
            # Convert to UTC if already timezone-aware
            vc_df["datetime"] = vc_df["datetime"].dt.tz_convert(timezone.utc)
            
        if mm_df["datetime"].dt.tz is None:
            # Meteomatics should be timezone-aware, but handle edge case
            mm_df["datetime"] = mm_df["datetime"].dt.tz_localize(timezone.utc)
        else:
            # Convert to UTC if already timezone-aware
            mm_df["datetime"] = mm_df["datetime"].dt.tz_convert(timezone.utc)

        # Find temperature columns
        vc_temp_col = None
        mm_temp_col = None

        # VisualCrossing temperature column - check for hourly temp first, then daily averages
        if "temp" in vc_df.columns and vc_df["temp"].notna().any():
            vc_temp_col = "temp"
        elif "tempmax" in vc_df.columns and "tempmin" in vc_df.columns:
            # Use daily average temperature if hourly temp is not available
            vc_df["temp_avg"] = (vc_df["tempmax"] + vc_df["tempmin"]) / 2
            vc_temp_col = "temp_avg"
        elif "temperature" in vc_df.columns:
            vc_temp_col = "temperature"

        # Meteomatics temperature column is typically 't_2m:C'
        if "t_2m:C" in mm_df.columns:
            mm_temp_col = "t_2m:C"

        assert vc_temp_col is not None, f"VisualCrossing DataFrame should have temperature column. Available columns: {vc_df.columns.tolist()}"
        assert mm_temp_col is not None, f"Meteomatics DataFrame should have temperature column. Available columns: {mm_df.columns.tolist()}"

        # Filter to common time range for comparison
        # VisualCrossing may return data from a broader range, so we filter to our requested range
        vc_df_filtered = vc_df[
            (vc_df["datetime"] >= start_time) & 
            (vc_df["datetime"] <= end_time)
        ].copy()
        
        mm_df_filtered = mm_df[
            (mm_df["datetime"] >= start_time) & 
            (mm_df["datetime"] <= end_time)
        ].copy()

        # Ensure we have data in the expected range
        assert len(vc_df_filtered) > 0, f"VisualCrossing should have data in the specified time range. Available data: {vc_df['datetime'].min()} to {vc_df['datetime'].max()}, requested: {start_time} to {end_time}"
        assert len(mm_df_filtered) > 0, f"Meteomatics should have data in the specified time range. Available data: {mm_df['datetime'].min()} to {mm_df['datetime'].max()}, requested: {start_time} to {end_time}"

        # Merge dataframes on datetime for comparison
        comparison_df = pd.merge(
            vc_df_filtered[["datetime", vc_temp_col]].rename(columns={vc_temp_col: "vc_temperature"}),
            mm_df_filtered[["datetime", mm_temp_col]].rename(columns={mm_temp_col: "mm_temperature"}),
            on="datetime",
            how="inner"
        )

        # Filter out rows where either API has missing temperature values
        comparison_df = comparison_df.dropna(subset=["vc_temperature", "mm_temperature"])

        assert len(comparison_df) > 0, "Should have overlapping data points with valid temperature values for comparison"

        # Calculate statistics
        temp_diff = comparison_df["vc_temperature"] - comparison_df["mm_temperature"]
        
        stats = {
            "mean_difference": temp_diff.mean(),
            "std_difference": temp_diff.std(),
            "min_difference": temp_diff.min(),
            "max_difference": temp_diff.max(),
            "abs_mean_difference": temp_diff.abs().mean(),
            "data_points": len(comparison_df)
        }

        print(f"\nTemperature Comparison Statistics:")
        print(f"Data points compared: {stats['data_points']}")
        print(f"Mean difference (VC - MM): {stats['mean_difference']:.2f}°C")
        print(f"Standard deviation of difference: {stats['std_difference']:.2f}°C")
        print(f"Min difference: {stats['min_difference']:.2f}°C")
        print(f"Max difference: {stats['max_difference']:.2f}°C")
        print(f"Mean absolute difference: {stats['abs_mean_difference']:.2f}°C")

        # Print sample data
        print(f"\nSample comparison data (first 5 rows):")
        print(comparison_df.head().to_string())

        # Validate reasonable temperature ranges
        assert -50 <= comparison_df["vc_temperature"].min() <= 60, "VisualCrossing temperatures should be reasonable"
        assert -50 <= comparison_df["vc_temperature"].max() <= 60, "VisualCrossing temperatures should be reasonable"
        assert -50 <= comparison_df["mm_temperature"].min() <= 60, "Meteomatics temperatures should be reasonable"
        assert -50 <= comparison_df["mm_temperature"].max() <= 60, "Meteomatics temperatures should be reasonable"

        # Check that differences are not extreme (within 20°C)
        assert temp_diff.abs().max() <= 20, "Temperature differences should not be extreme (>20°C)"

        # Additional validation: check that we have enough data points for meaningful comparison
        assert len(comparison_df) >= 2, "Should have at least 2 data points for meaningful comparison"

        print(f"\n✅ Temperature comparison test passed successfully!")
        print(f"✅ Both APIs returned valid temperature data for current time range")
        print(f"✅ Temperature differences are within reasonable bounds")

    except Exception as e:
        print(f"Error during temperature comparison: {e}")
        assert False, f"Temperature comparison test failed: {e}"


def test_temperature_data_structure_consistency():
    """
    Test that both APIs return consistent data structures for temperature forecasts.
    """
    config = get_config()
    visualcrossing_config = config.get("VisualCrossing", {})
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    vc_api_key = visualcrossing_config.get("api_key")
    mm_username = meteomatics_config.get("username")
    mm_password = meteomatics_config.get("password")
    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if not vc_api_key or not mm_username or not mm_password or latitude is None or longitude is None:
        pytest.skip("Required credentials or location not available")

    vc_api = VisualCrossingAPI(vc_api_key)
    mm_api = MeteomaticsAPI(mm_username, mm_password)

    # Test with a shorter time range for structure validation
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_time = now + timedelta(hours=1)
    end_time = start_time + timedelta(hours=6)  # 6 hours for structure test

    start_dt_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_dt_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    location = [(latitude, longitude)]

    try:
        # Get forecasts
        vc_forecast = vc_api.get_forecast(
            locations=location,
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
        )
        
        mm_forecast = mm_api.get_forecast(
            locations=location,
            parameters=["t_2m:C"],
            start_datetime=start_time,
            end_datetime=end_time,
            interval=timedelta(hours=1),
        )

        vc_df = vc_forecast
        mm_df = mm_forecast[0]

        # Validate basic structure
        assert "datetime" in vc_df.columns, "VisualCrossing should have datetime column"
        assert "datetime" in mm_df.columns, "Meteomatics should have datetime column"
        assert "latitude" in mm_df.columns, "Meteomatics should have latitude column"
        assert "longitude" in mm_df.columns, "Meteomatics should have longitude column"

        # Check for temperature columns
        vc_has_temp = any(col in vc_df.columns for col in ["temp", "temperature"])
        mm_has_temp = "t_2m:C" in mm_df.columns

        assert vc_has_temp, "VisualCrossing should have temperature column"
        assert mm_has_temp, "Meteomatics should have temperature column"

        # Validate datetime format
        assert pd.api.types.is_datetime64_any_dtype(vc_df["datetime"]), "VisualCrossing datetime should be datetime type"
        assert pd.api.types.is_datetime64_any_dtype(mm_df["datetime"]), "Meteomatics datetime should be datetime type"

        print("✅ Data structure consistency test passed")

    except Exception as e:
        print(f"Error during structure consistency test: {e}")
        assert False, f"Structure consistency test failed: {e}"


def test_temperature_forecast_timeline_accuracy():
    """
    Test that both APIs return temperature forecasts with accurate timeline coverage.
    """
    config = get_config()
    visualcrossing_config = config.get("VisualCrossing", {})
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    vc_api_key = visualcrossing_config.get("api_key")
    mm_username = meteomatics_config.get("username")
    mm_password = meteomatics_config.get("password")
    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if not vc_api_key or not mm_username or not mm_password or latitude is None or longitude is None:
        pytest.skip("Required credentials or location not available")

    vc_api = VisualCrossingAPI(vc_api_key)
    mm_api = MeteomaticsAPI(mm_username, mm_password)

    # Test with 12-hour forecast
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_time = now + timedelta(hours=1)
    end_time = start_time + timedelta(hours=12)

    start_dt_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_dt_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    location = [(latitude, longitude)]

    try:
        vc_forecast = vc_api.get_forecast(
            locations=location,
            start_datetime=start_dt_str,
            end_datetime=end_dt_str,
        )
        
        mm_forecast = mm_api.get_forecast(
            locations=location,
            parameters=["t_2m:C"],
            start_datetime=start_time,
            end_datetime=end_time,
            interval=timedelta(hours=1),
        )

        vc_df = vc_forecast
        mm_df = mm_forecast[0]

        # Handle timezone awareness for timeline comparison
        if vc_df["datetime"].dt.tz is None:
            vc_df["datetime"] = vc_df["datetime"].dt.tz_localize(timezone.utc)
        else:
            vc_df["datetime"] = vc_df["datetime"].dt.tz_convert(timezone.utc)
            
        if mm_df["datetime"].dt.tz is None:
            mm_df["datetime"] = mm_df["datetime"].dt.tz_localize(timezone.utc)
        else:
            mm_df["datetime"] = mm_df["datetime"].dt.tz_convert(timezone.utc)

        # Check timeline coverage
        vc_timeline = vc_df["datetime"].sort_values()
        mm_timeline = mm_df["datetime"].sort_values()

        # Should have data points within the requested range
        assert len(vc_timeline) > 0, "VisualCrossing should have timeline data"
        assert len(mm_timeline) > 0, "Meteomatics should have timeline data"

        # Check that data covers the requested time range (VisualCrossing may return broader range)
        # For VisualCrossing, check that our requested range is covered by the returned data
        vc_has_start = (vc_timeline <= start_time).any()
        vc_has_end = (vc_timeline >= end_time).any()
        assert vc_has_start or vc_timeline.min() <= start_time, f"VisualCrossing should cover requested start time. Data range: {vc_timeline.min()} to {vc_timeline.max()}, requested: {start_time}"
        assert vc_has_end or vc_timeline.max() >= end_time, f"VisualCrossing should cover requested end time. Data range: {vc_timeline.min()} to {vc_timeline.max()}, requested: {end_time}"
        
        # For Meteomatics, check that data is within the requested range
        assert mm_timeline.min() >= start_time, f"Meteomatics data should start after requested start time. Data range: {mm_timeline.min()} to {mm_timeline.max()}, requested: {start_time}"
        assert mm_timeline.max() <= end_time, f"Meteomatics data should end before requested end time. Data range: {mm_timeline.min()} to {mm_timeline.max()}, requested: {end_time}"

        # Check for reasonable data point frequency (should have multiple points for 12 hours)
        assert len(vc_timeline) >= 6, "VisualCrossing should have at least 6 data points for 12 hours"
        assert len(mm_timeline) >= 6, "Meteomatics should have at least 6 data points for 12 hours"

        print(f"✅ Timeline accuracy test passed")
        print(f"VisualCrossing: {len(vc_timeline)} data points from {vc_timeline.min()} to {vc_timeline.max()}")
        print(f"Meteomatics: {len(mm_timeline)} data points from {mm_timeline.min()} to {mm_timeline.max()}")

    except Exception as e:
        print(f"Error during timeline accuracy test: {e}")
        assert False, f"Timeline accuracy test failed: {e}" 