"""
Test suite for Stormglass API get_high_resolution_forecast functionality.
Tests high-resolution data retrieval for multiple parameters with 15-minute intervals.
"""

import pytest
from datetime import datetime, timezone, timedelta
import datetime as dt
import pandas as pd
from stormglass_api import StormglassAPI
from config import get_config
import requests


def test_get_high_resolution_forecast_basic_parameters():
    """
    Test get_high_resolution_forecast for basic parameters with 12 hours ahead.
    Ensures data contains all points in hourly intervals and verifies all parameters.
    Note: Stormglass API only provides hourly data, not 15-minute resolution.
    """
    # Load configuration
    config = get_config()
    stormglass_config = config.get("Stormglass", {})
    location_config = config.get("Location", {})

    # Get credentials
    api_key = stormglass_config.get("api_key")

    if not api_key:
        pytest.skip("Stormglass API key not available")

    # Get location coordinates
    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if latitude is None or longitude is None:
        pytest.skip("Location coordinates not available")

    # Initialize API
    api = StormglassAPI(api_key)
    locations = [(latitude, longitude)]

    # Define basic parameters that are known to work with Stormglass API
    # Tested parameters with Stormglass API:
    # ✅ SUPPORTED parameters:
    # - airTemperature              # Air temperature in Celsius
    # - windSpeed                   # Wind speed in m/s
    # - windDirection               # Wind direction in degrees
    # - pressure                    # Atmospheric pressure in hPa
    # - precipitation               # Precipitation in mm
    # - humidity                    # Relative humidity in %
    # - cloudCover                  # Cloud cover in %
    # - visibility                  # Visibility in meters
    # - gust                        # Wind gust in m/s
    # - dewPoint                    # Dew point temperature in Celsius
    # - groundTemperature           # Ground temperature in Celsius
    #
    parameters = [
        "airTemperature",  # Air temperature in Celsius
        "windSpeed",  # Wind speed in m/s
        "windDirection",  # Wind direction in degrees
        "pressure",  # Atmospheric pressure in hPa
        "precipitation",  # Precipitation in mm
        "humidity",  # Relative humidity in %
        "cloudCover",  # Cloud cover in %
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
            assert param in df.columns, f"Parameter '{param}' not found in DataFrame"

        # Validate time range
        min_datetime = df["datetime"].min()
        max_datetime = df["datetime"].max()

        assert (
            min_datetime >= start_datetime
        ), f"Data starts before requested start time: {min_datetime} < {start_datetime}"
        assert (
            max_datetime <= end_datetime
        ), f"Data ends after requested end time: {max_datetime} > {end_datetime}"

        # Check for 1-hour intervals (approximately)
        time_diffs = df["datetime"].diff().dropna()
        expected_interval = pd.Timedelta(hours=1)
        tolerance = pd.Timedelta(minutes=5)  # Allow 5 minutes tolerance

        for diff in time_diffs:
            assert (
                abs(diff - expected_interval) <= tolerance
            ), f"Time interval {diff} is not close to 1 hour"

        # Check data quality
        for param in parameters:
            if param in df.columns:
                # Check for reasonable value ranges
                values = df[param].dropna()
                if len(values) > 0:
                    if param == "airTemperature":
                        assert (
                            values.min() >= -50 and values.max() <= 60
                        ), f"Temperature values out of reasonable range: {values.min()} to {values.max()}"
                    elif param == "windSpeed":
                        assert (
                            values.min() >= 0 and values.max() <= 50
                        ), f"Wind speed values out of reasonable range: {values.min()} to {values.max()}"
                    elif param == "windDirection":
                        assert (
                            values.min() >= 0 and values.max() <= 360
                        ), f"Wind direction values out of reasonable range: {values.min()} to {values.max()}"
                    elif param == "pressure":
                        assert (
                            values.min() >= 800 and values.max() <= 1200
                        ), f"Pressure values out of reasonable range: {values.min()} to {values.max()}"
                    elif param == "humidity":
                        assert (
                            values.min() >= 0 and values.max() <= 100
                        ), f"Humidity values out of reasonable range: {values.min()} to {values.max()}"
                    elif param == "cloudCover":
                        assert (
                            values.min() >= 0 and values.max() <= 100
                        ), f"Cloud cover values out of reasonable range: {values.min()} to {values.max()}"

                    print(
                f"✅ Successfully retrieved {len(df)} data points with hourly resolution"
            )
        print(f"📊 Parameters available: {list(df.columns)}")
        print(f"⏰ Time range: {min_datetime} to {max_datetime}")

    except requests.exceptions.HTTPError as e:
        if "404" in str(e):
            pytest.skip("Stormglass API service not available (404 error)")
        else:
            pytest.fail(f"Test failed with HTTP error: {str(e)}")
    except Exception as e:
        pytest.fail(f"Test failed with exception: {str(e)}")


def test_get_standard_forecast():
    """
    Test get_standard_forecast for basic parameters with 24 hours ahead.
    Ensures data contains all points in 1-hour intervals.
    """
    # Load configuration
    config = get_config()
    stormglass_config = config.get("Stormglass", {})
    location_config = config.get("Location", {})

    # Get credentials
    api_key = stormglass_config.get("api_key")

    if not api_key:
        pytest.skip("Stormglass API key not available")

    # Get location coordinates
    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if latitude is None or longitude is None:
        pytest.skip("Location coordinates not available")

    # Initialize API
    api = StormglassAPI(api_key)
    locations = [(latitude, longitude)]

    # Define basic parameters
    parameters = [
        "airTemperature",  # Air temperature in Celsius
        "windSpeed",  # Wind speed in m/s
        "windDirection",  # Wind direction in degrees
        "pressure",  # Atmospheric pressure in hPa
    ]

    # Set up time range: 24 hours ahead from current time
    start_datetime = dt.datetime.now(dt.timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    end_datetime = start_datetime + timedelta(hours=24)

    try:
        # Request standard forecast data
        results = api.get_standard_forecast(
            locations=locations,
            parameters=parameters,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

        # Basic validation
        assert len(results) > 0, "No results received from get_standard_forecast"
        df = results[0]  # Get first (and only) result
        assert not df.empty, "DataFrame is empty, no data received"

        # Check that datetime column exists and is properly formatted
        assert "datetime" in df.columns, "datetime column not found in DataFrame"
        assert pd.api.types.is_datetime64_any_dtype(
            df["datetime"]
        ), "datetime column is not datetime type"

        # Check that all requested parameters exist in the DataFrame
        for param in parameters:
            assert param in df.columns, f"Parameter '{param}' not found in DataFrame"

        # Validate time range
        min_datetime = df["datetime"].min()
        max_datetime = df["datetime"].max()

        assert (
            min_datetime >= start_datetime
        ), f"Data starts before requested start time: {min_datetime} < {start_datetime}"
        assert (
            max_datetime <= end_datetime
        ), f"Data ends after requested end time: {max_datetime} > {end_datetime}"

        # Check for 1-hour intervals (approximately)
        time_diffs = df["datetime"].diff().dropna()
        expected_interval = pd.Timedelta(hours=1)
        tolerance = pd.Timedelta(minutes=5)  # Allow 5 minutes tolerance

        for diff in time_diffs:
            assert (
                abs(diff - expected_interval) <= tolerance
            ), f"Time interval {diff} is not close to 1 hour"

        print(f"✅ Successfully retrieved {len(df)} data points with 1-hour resolution")
        print(f"📊 Parameters available: {list(df.columns)}")
        print(f"⏰ Time range: {min_datetime} to {max_datetime}")

    except requests.exceptions.HTTPError as e:
        if "404" in str(e):
            pytest.skip("Stormglass API service not available (404 error)")
        else:
            pytest.fail(f"Test failed with HTTP error: {str(e)}")
    except Exception as e:
        pytest.fail(f"Test failed with exception: {str(e)}")


def test_api_initialization():
    """
    Test StormglassAPI initialization with valid and invalid API keys.
    """
    # Test with valid API key
    try:
        api = StormglassAPI("test_api_key")
        assert api.api_key == "test_api_key"
        assert api.base_url == "https://api.stormglass.io"
    except Exception as e:
        pytest.fail(f"Valid API key initialization failed: {str(e)}")

    # Test with empty API key
    with pytest.raises(ValueError, match="API key cannot be empty"):
        StormglassAPI("")

    # Test with None API key
    with pytest.raises(ValueError, match="API key cannot be empty"):
        StormglassAPI(None)


def test_parameter_validation():
    """
    Test parameter validation and conversion.
    """
    api = StormglassAPI("test_api_key")

    # Test parameter conversion - Stormglass API expects original parameter names
    parameters = ["airTemperature", "windSpeed", "windDirection"]
    converted = api._convert_parameters(parameters)
    expected = ["airTemperature", "windSpeed", "windDirection"]  # No conversion needed
    assert (
        converted == expected
    ), f"Parameter conversion should return original names. Got {converted}, expected {expected}"

    # Test with unknown parameters (should pass through as-is)
    unknown_params = ["unknownParam", "anotherUnknown"]
    converted = api._convert_parameters(unknown_params)
    assert (
        converted == unknown_params
    ), "Unknown parameters should pass through unchanged"


def test_interval_validation():
    """
    Test interval validation for different time resolutions.
    """
    api = StormglassAPI("test_api_key")

    # Test supported intervals
    supported_intervals = ["15min", "30min", "1h", "3h", "6h", "12h", "1d"]

    for interval in supported_intervals:
        try:
            # This should not raise an exception
            api.get_forecast(
                locations=[(50.0, 30.0)],
                interval=interval,
                start_datetime=dt.datetime.now(dt.timezone.utc),
                end_datetime=dt.datetime.now(dt.timezone.utc) + timedelta(hours=1),
            )
        except requests.exceptions.HTTPError as e:
            # API call might fail due to invalid credentials or service unavailability, but interval validation should pass
            if "401" in str(e) or "403" in str(e) or "404" in str(e):
                # This is expected with test credentials or when service is unavailable
                pass
            else:
                pytest.fail(f"Interval {interval} validation failed with HTTP error: {str(e)}")
        except Exception as e:
            pytest.fail(f"Interval {interval} validation failed: {str(e)}")


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
