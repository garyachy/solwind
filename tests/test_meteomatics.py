"""
Comprehensive test suite for Meteomatics API functionality.
Tests API access, datetime column handling, and proper timestamp generation
based on specified periods and intervals.
"""

import pytest
from datetime import datetime, timezone, timedelta
import datetime as dt
import pandas as pd
from meteomatics_api import MeteomaticsAPI
from meteomatics_draw import MeteomaticsForecastDraw
from combined_draw import CombinedForecastDraw
from visualcrossing_api import VisualCrossingAPI
from config import get_config


def test_meteomatics_access():
    """Test basic Meteomatics API access and data retrieval."""
    # Load configuration
    config = get_config()
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    # Specify credentials for the Meteomatics API
    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")

    if not username or not password:
        pytest.skip("Meteomatics credentials not available")

    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if latitude is None or longitude is None:
        pytest.skip("Location coordinates not available")

    # Initialize MeteomaticsAPI
    api = MeteomaticsAPI(username, password)

    # Prepare request parameters
    locations = [(latitude, longitude)]
    parameters = ["t_2m:C"]  # Temperature
    startdate = dt.datetime.now(dt.timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )  # Current time, rounded to the hour
    enddate = startdate  # Single time point for testing

    try:
        # Request time series data using the API class
        results = api.get_forecast(
            locations=locations,
            parameters=parameters,
            start_datetime=startdate,
            end_datetime=enddate,
        )
        
        assert len(results) > 0, "No results received"
        df = results[0]  # Get first (and only) result
        assert not df.empty, "Dataframe is empty, no data received"
        print("Weather data successfully received:")
        print(df)

    except Exception as e:
        print(e)
        assert False, f"An error occurred: {e}"


def test_meteomatics_datetime_column_structure():
    """Test that Meteomatics API returns proper datetime column structure."""
    config = get_config()
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")

    if not username or not password:
        pytest.skip("Meteomatics credentials not available")

    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if latitude is None or longitude is None:
        pytest.skip("Location coordinates not available")

    api = MeteomaticsAPI(username, password)
    locations = [(latitude, longitude)]
    parameters = ["t_2m:C"]

    # Test with a specific time range
    start_datetime = dt.datetime.now(dt.timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    end_datetime = start_datetime + timedelta(hours=6)

    results = api.get_forecast(
        locations=locations,
        parameters=parameters,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        interval=timedelta(hours=1)
    )

    assert len(results) > 0, "No results received"
    df = results[0]
    assert not df.empty, "DataFrame is empty"

    # Check that datetime column exists
    assert "datetime" in df.columns, "datetime column not found in DataFrame"
    
    # Check that datetime column is of proper type
    assert pd.api.types.is_datetime64_any_dtype(df["datetime"]), "datetime column is not datetime type"
    
    # Check that datetime values are timezone-aware
    assert df["datetime"].dt.tz is not None, "datetime values are not timezone-aware"
    
    # Check that datetime values are in UTC
    assert df["datetime"].dt.tz == timezone.utc, "datetime values are not in UTC"


def test_meteomatics_datetime_timestamps_accuracy():
    """Test that Meteomatics API returns timestamps that match the specified period."""
    config = get_config()
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")

    if not username or not password:
        pytest.skip("Meteomatics credentials not available")

    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if latitude is None or longitude is None:
        pytest.skip("Location coordinates not available")

    api = MeteomaticsAPI(username, password)
    locations = [(latitude, longitude)]
    parameters = ["t_2m:C"]

    # Test with a specific time range and interval
    start_datetime = dt.datetime.now(dt.timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    end_datetime = start_datetime + timedelta(hours=3)
    interval = timedelta(hours=1)

    results = api.get_forecast(
        locations=locations,
        parameters=parameters,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        interval=interval
    )

    assert len(results) > 0, "No results received"
    df = results[0]
    assert not df.empty, "DataFrame is empty"

    # Check that datetime values are within the specified range
    min_datetime = df["datetime"].min()
    max_datetime = df["datetime"].max()
    
    assert min_datetime >= start_datetime, f"Earliest timestamp {min_datetime} is before start {start_datetime}"
    assert max_datetime <= end_datetime, f"Latest timestamp {max_datetime} is after end {end_datetime}"

    # Check that timestamps follow the specified interval
    if len(df) > 1:
        time_diffs = df["datetime"].diff().dropna()
        expected_interval = pd.Timedelta(interval)
        
        # Allow for small tolerance due to API response variations
        tolerance = pd.Timedelta(minutes=5)
        
        for diff in time_diffs:
            assert abs(diff - expected_interval) <= tolerance, \
                f"Time interval {diff} does not match expected {expected_interval}"


def test_meteomatics_datetime_different_intervals():
    """Test that Meteomatics API handles different intervals correctly."""
    config = get_config()
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")

    if not username or not password:
        pytest.skip("Meteomatics credentials not available")

    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if latitude is None or longitude is None:
        pytest.skip("Location coordinates not available")

    api = MeteomaticsAPI(username, password)
    locations = [(latitude, longitude)]
    parameters = ["t_2m:C"]

    start_datetime = dt.datetime.now(dt.timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    end_datetime = start_datetime + timedelta(hours=6)

    # Test different intervals including 15-minute resolution
    intervals = [
        timedelta(minutes=15),  # 15 minutes (new default)
        timedelta(hours=1),     # 1 hour
        timedelta(hours=3),     # 3 hours
        timedelta(hours=6)      # 6 hours
    ]

    for interval in intervals:
        results = api.get_forecast(
            locations=locations,
            parameters=parameters,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            interval=interval
        )

        assert len(results) > 0, f"No results received for interval {interval}"
        df = results[0]
        assert not df.empty, f"DataFrame is empty for interval {interval}"

        # Check that datetime column exists and is properly formatted
        assert "datetime" in df.columns, f"datetime column not found for interval {interval}"
        assert pd.api.types.is_datetime64_any_dtype(df["datetime"]), \
            f"datetime column is not datetime type for interval {interval}"

        # Check that we have reasonable number of data points
        expected_points = int((end_datetime - start_datetime) / interval) + 1
        tolerance = 2  # Allow for some variation in API response
        
        assert len(df) >= max(1, expected_points - tolerance), \
            f"Too few data points for interval {interval}: got {len(df)}, expected at least {max(1, expected_points - tolerance)}"


def test_meteomatics_datetime_timezone_consistency():
    """Test that Meteomatics API consistently returns UTC timestamps."""
    config = get_config()
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")

    if not username or not password:
        pytest.skip("Meteomatics credentials not available")

    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if latitude is None or longitude is None:
        pytest.skip("Location coordinates not available")

    api = MeteomaticsAPI(username, password)
    locations = [(latitude, longitude)]
    parameters = ["t_2m:C"]

    start_datetime = dt.datetime.now(dt.timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    end_datetime = start_datetime + timedelta(hours=2)

    results = api.get_forecast(
        locations=locations,
        parameters=parameters,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        interval=timedelta(hours=1)
    )

    assert len(results) > 0, "No results received"
    df = results[0]
    assert not df.empty, "DataFrame is empty"

    # Check that all datetime values are in UTC
    for dt_val in df["datetime"]:
        assert dt_val.tzinfo == timezone.utc, f"Timestamp {dt_val} is not in UTC"


def test_meteomatics_datetime_no_validdate_column():
    """Test that the API properly handles cases where validdate column is not present."""
    config = get_config()
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")

    if not username or not password:
        pytest.skip("Meteomatics credentials not available")

    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if latitude is None or longitude is None:
        pytest.skip("Location coordinates not available")

    api = MeteomaticsAPI(username, password)
    locations = [(latitude, longitude)]
    parameters = ["t_2m:C"]

    start_datetime = dt.datetime.now(dt.timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    end_datetime = start_datetime + timedelta(hours=1)

    results = api.get_forecast(
        locations=locations,
        parameters=parameters,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        interval=timedelta(hours=1)
    )

    assert len(results) > 0, "No results received"
    df = results[0]
    
    # Check that validdate column is not present in the final DataFrame
    assert "validdate" not in df.columns, "validdate column should be removed from final DataFrame"
    
    # Check that datetime column exists
    assert "datetime" in df.columns, "datetime column should be present in final DataFrame"
    
    # Check that datetime column is properly formatted
    assert pd.api.types.is_datetime64_any_dtype(df["datetime"]), "datetime column should be datetime type"


def test_meteomatics_datetime_period_accuracy():
    """Test that Meteomatics API returns proper datetime columns with expected timestamps based on period specified."""
    config = get_config()
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")

    if not username or not password:
        pytest.skip("Meteomatics credentials not available")

    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if latitude is None or longitude is None:
        pytest.skip("Location coordinates not available")

    api = MeteomaticsAPI(username, password)
    locations = [(latitude, longitude)]
    parameters = ["t_2m:C"]  # Use only basic parameter for trial account

    # Test different periods and intervals
    test_cases = [
        {
            "start": dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0),
            "end": dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1),
            "interval": timedelta(hours=1),
            "expected_points": 2
        },
        {
            "start": dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0),
            "end": dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0) + timedelta(hours=6),
            "interval": timedelta(hours=2),
            "expected_points": 4
        },
        {
            "start": dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0),
            "end": dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0) + timedelta(hours=12),
            "interval": timedelta(hours=3),
            "expected_points": 5
        }
    ]

    for i, test_case in enumerate(test_cases):
        start_datetime = test_case["start"]
        end_datetime = test_case["end"]
        interval = test_case["interval"]
        expected_points = test_case["expected_points"]

        results = api.get_forecast(
            locations=locations,
            parameters=parameters,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            interval=interval
        )

        assert len(results) > 0, f"No results received for test case {i+1}"
        df = results[0]
        assert not df.empty, f"DataFrame is empty for test case {i+1}"

        # Check datetime column structure
        assert "datetime" in df.columns, f"datetime column not found for test case {i+1}"
        assert pd.api.types.is_datetime64_any_dtype(df["datetime"]), \
            f"datetime column is not datetime type for test case {i+1}"
        assert df["datetime"].dt.tz == timezone.utc, \
            f"datetime values are not in UTC for test case {i+1}"

        # Check that timestamps match the specified period
        min_datetime = df["datetime"].min()
        max_datetime = df["datetime"].max()
        
        assert min_datetime >= start_datetime, \
            f"Earliest timestamp {min_datetime} is before start {start_datetime} for test case {i+1}"
        assert max_datetime <= end_datetime, \
            f"Latest timestamp {max_datetime} is after end {end_datetime} for test case {i+1}"

        # Check that we have the expected number of data points (with tolerance)
        tolerance = 1
        assert len(df) >= max(1, expected_points - tolerance), \
            f"Too few data points for test case {i+1}: got {len(df)}, expected at least {max(1, expected_points - tolerance)}"

        # Check that timestamps follow the specified interval
        if len(df) > 1:
            time_diffs = df["datetime"].diff().dropna()
            expected_interval = pd.Timedelta(interval)
            tolerance_timedelta = pd.Timedelta(minutes=5)
            
            for diff in time_diffs:
                assert abs(diff - expected_interval) <= tolerance_timedelta, \
                    f"Time interval {diff} does not match expected {expected_interval} for test case {i+1}"

        # Check that all parameters are present
        for param in parameters:
            assert param in df.columns, f"Parameter {param} not found in DataFrame for test case {i+1}"

        print(f"Test case {i+1} passed: {len(df)} data points from {min_datetime} to {max_datetime}")


def test_meteomatics_api_initialization():
    """Test that MeteomaticsAPI can be initialized with valid credentials."""
    config = get_config()
    meteomatics_config = config.get("Meteomatics", {})
    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")
    
    if username and password:
        api = MeteomaticsAPI(username, password)
        assert api.username == username
        assert api.password == password
    else:
        pytest.skip("Meteomatics credentials not available")


def test_meteomatics_draw_initialization():
    """Test that MeteomaticsForecastDraw can be initialized."""
    config = get_config()
    meteomatics_config = config.get("Meteomatics", {})
    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")
    
    if username and password:
        api = MeteomaticsAPI(username, password)
        draw = MeteomaticsForecastDraw(api)
        assert draw.api == api
    else:
        pytest.skip("Meteomatics credentials not available")


def test_combined_draw_initialization():
    """Test that CombinedForecastDraw can be initialized with both APIs."""
    config = get_config()
    meteomatics_config = config.get("Meteomatics", {})
    visualcrossing_config = config.get("VisualCrossing", {})
    
    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")
    api_key = visualcrossing_config.get("api_key")
    
    if username and password and api_key:
        mm_api = MeteomaticsAPI(username, password)
        vc_api = VisualCrossingAPI(api_key)
        combined = CombinedForecastDraw(vc_api, mm_api)
        assert combined.visualcrossing_api == vc_api
        assert combined.meteomatics_api == mm_api
    else:
        pytest.skip("Both API credentials not available")


if __name__ == "__main__":
    # Run tests directly
    test_meteomatics_access()
    test_meteomatics_datetime_column_structure()
    test_meteomatics_datetime_timestamps_accuracy()
    test_meteomatics_datetime_different_intervals()
    test_meteomatics_datetime_timezone_consistency()
    test_meteomatics_datetime_no_validdate_column()
    test_meteomatics_datetime_period_accuracy()
    test_meteomatics_api_initialization()
    test_meteomatics_draw_initialization()
    test_combined_draw_initialization()
    print("All Meteomatics tests passed!")
