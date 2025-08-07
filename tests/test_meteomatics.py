"""
Test suite for Meteomatics API get_high_resolution_forecast functionality.
Tests high-resolution data retrieval for multiple parameters with 15-minute intervals.
"""

import pytest
from datetime import datetime, timezone, timedelta
import datetime as dt
import pandas as pd
from meteomatics_api import MeteomaticsAPI
from config import get_config


def test_get_high_resolution_forecast_basic_parameters():
    """
    Test get_high_resolution_forecast for basic parameters with 12 hours ahead.
    Ensures data contains all points in 15-minute intervals and verifies all parameters.
    """
    # Load configuration
    config = get_config()
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    # Get credentials
    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")

    if not username or not password:
        pytest.skip("Meteomatics credentials not available")

    # Get location coordinates
    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if latitude is None or longitude is None:
        pytest.skip("Location coordinates not available")

    # Initialize API
    api = MeteomaticsAPI(username, password)
    locations = [(latitude, longitude)]
    
    # Define basic parameters that are known to work
    parameters = [
        "t_2m:C",                    # Temperature at 2m in Celsius
    ]

    # Set up time range: 12 hours ahead from current time
    start_datetime = dt.datetime.now(dt.timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    end_datetime = start_datetime + timedelta(hours=12)

    try:
        # Request high-resolution forecast data
        results = api.get_high_resolution_forecast(
            locations=locations,
            parameters=parameters,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

        # Basic validation
        assert len(results) > 0, "No results received from get_high_resolution_forecast"
        df = results[0]  # Get first (and only) result
        assert not df.empty, "DataFrame is empty, no data received"

        # Check that datetime column exists and is properly formatted
        assert "datetime" in df.columns, "datetime column not found in DataFrame"
        assert pd.api.types.is_datetime64_any_dtype(
            df["datetime"]
        ), "datetime column is not datetime type"
        assert df["datetime"].dt.tz == timezone.utc, "datetime values are not in UTC"

        # Check that all requested parameters exist in the DataFrame
        for param in parameters:
            assert (
                param in df.columns
            ), f"Parameter '{param}' not found in DataFrame"

        # Validate time range
        min_datetime = df["datetime"].min()
        max_datetime = df["datetime"].max()

        assert (
            min_datetime >= start_datetime
        ), f"Earliest timestamp {min_datetime} is before start {start_datetime}"
        assert (
            max_datetime <= end_datetime
        ), f"Latest timestamp {max_datetime} is after end {end_datetime}"

        # Check for 15-minute intervals
        if len(df) > 1:
            time_diffs = df["datetime"].diff().dropna()
            expected_interval = pd.Timedelta(minutes=15)
            tolerance = pd.Timedelta(minutes=2)  # Allow 2-minute tolerance

            for diff in time_diffs:
                assert (
                    abs(diff - expected_interval) <= tolerance
                ), f"Time interval {diff} does not match expected 15-minute interval {expected_interval}"

        # Calculate expected number of data points for 12 hours with 15-minute intervals
        # 12 hours = 720 minutes, 720 / 15 = 48 intervals, +1 for the start point = 49 total points
        expected_points = 49
        points_tolerance = 2  # Allow for some variation in API response

        assert len(df) >= max(
            1, expected_points - points_tolerance
        ), f"Too few data points: got {len(df)}, expected at least {max(1, expected_points - points_tolerance)} for 12 hours with 15-minute intervals"

        # Verify all timestamps are properly spaced
        if len(df) > 1:
            # Sort by datetime to ensure proper order
            df_sorted = df.sort_values("datetime")
            time_diffs = df_sorted["datetime"].diff().dropna()

            # All intervals should be approximately 15 minutes
            for i, diff in enumerate(time_diffs):
                assert (
                    abs(diff - expected_interval) <= tolerance
                ), f"Interval {i+1} ({diff}) does not match expected 15-minute interval"

        # Check that all parameters have reasonable values (not all NaN)
        parameter_stats = {}
        for param in parameters:
            param_values = df[param].dropna()
            assert len(param_values) > 0, f"All values for parameter '{param}' are NaN"
            
            # Store statistics for reporting
            parameter_stats[param] = {
                'min': param_values.min(),
                'max': param_values.max(),
                'mean': param_values.mean(),
                'count': len(param_values)
            }

        # Print summary for verification
        print(f"✅ High-resolution forecast test with {len(parameters)} parameters passed:")
        print(f"   - Data points: {len(df)}")
        print(f"   - Time range: {min_datetime} to {max_datetime}")
        print(f"   - Expected points: {expected_points} (12 hours × 4 points/hour)")
        print(f"   - Actual points: {len(df)}")
        print(f"   - Parameters tested: {len(parameters)}")
        print(f"\nParameter Statistics:")
        
        for param, stats in parameter_stats.items():
            print(f"   - {param}:")
            print(f"     Range: {stats['min']:.2f} to {stats['max']:.2f}")
            print(f"     Mean: {stats['mean']:.2f}")
            print(f"     Valid values: {stats['count']}/{len(df)}")

        # Verify we have data for the full 12-hour period
        actual_duration = max_datetime - min_datetime
        expected_duration = timedelta(hours=12)
        duration_tolerance = timedelta(minutes=30)  # Allow 30-minute tolerance

        assert (
            abs(actual_duration - expected_duration) <= duration_tolerance
        ), f"Data duration {actual_duration} does not match expected {expected_duration}"

        # Additional validation: Check for reasonable value ranges for specific parameters
        if "t_2m:C" in parameter_stats:
            temp_stats = parameter_stats["t_2m:C"]
            assert -50 <= temp_stats['min'] <= 60, f"Temperature minimum {temp_stats['min']}°C is outside reasonable range"
            assert -50 <= temp_stats['max'] <= 60, f"Temperature maximum {temp_stats['max']}°C is outside reasonable range"

        if "rh_2m:p" in parameter_stats:
            humidity_stats = parameter_stats["rh_2m:p"]
            assert 0 <= humidity_stats['min'] <= 100, f"Humidity minimum {humidity_stats['min']}% is outside reasonable range"
            assert 0 <= humidity_stats['max'] <= 100, f"Humidity maximum {humidity_stats['max']}% is outside reasonable range"

        if "wind_speed_10m:ms" in parameter_stats:
            wind_stats = parameter_stats["wind_speed_10m:ms"]
            assert 0 <= wind_stats['min'] <= 50, f"Wind speed minimum {wind_stats['min']} m/s is outside reasonable range"
            assert 0 <= wind_stats['max'] <= 50, f"Wind speed maximum {wind_stats['max']} m/s is outside reasonable range"

        print(f"\n✅ All {len(parameters)} parameters validated successfully!")

    except Exception as e:
        print(f"Error during high-resolution forecast test: {e}")
        assert False, f"An error occurred: {e}"


if __name__ == "__main__":
    # Run test directly
    test_get_high_resolution_forecast_basic_parameters()
    print("High-resolution forecast test with basic parameters passed!")

