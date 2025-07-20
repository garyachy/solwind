from config import get_config
import pytest
import pandas as pd
from datetime import datetime, timedelta, timezone
from openmeteo import (
    OpenMeteo,
    OpenMeteoModelType,
    OpenMeteo15mParam,
    OpenMeteo1hourParam,
)


@pytest.fixture(scope="module")
def openmeteo_test_data():
    config = get_config()
    openmeteo_config = config.get("Openmeteo", {})
    location_config = config.get("Location", {})

    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")
    api_key = openmeteo_config.get("api_key")

    assert (
        latitude is not None
    ), "Latitude not found in config for tests. Please check your test configuration."
    assert (
        longitude is not None
    ), "Longitude not found in config for tests. Please check your test configuration."

    openmeteo_instance = OpenMeteo(api_key)
    return latitude, longitude, openmeteo_instance


def test_openmeteo_load_data(openmeteo_test_data):
    latitude, longitude, openmeteo_instance = openmeteo_test_data

    start_dt = datetime.now(timezone.utc) - timedelta(days=1)
    end_dt = datetime.now(timezone.utc)

    data = openmeteo_instance.load_data(
        latitudes=latitude,
        longitudes=longitude,
        time_delta=15,  # Added missing mandatory argument, using 15 for 15-minute data
        start_time=start_dt,  # Corrected parameter name from start_date
        end_time=end_dt,  # Corrected parameter name from end_date
    )

    assert isinstance(data, pd.DataFrame), "Data should be a Pandas DataFrame"
    assert not data.empty, "DataFrame should not be empty"
    assert "datetime" in data.columns, "'datetime' column should be present"

    # Check if some default 15-min parameters are present
    if OpenMeteo.params_minutely_15:  # Default list of 15-min parameters
        # Check for at least one common 15-min parameter
        assert any(
            param in data.columns
            for param in [
                OpenMeteo15mParam.temperature_air_2.value,
                OpenMeteo15mParam.gti.value,
            ]
        ), "Expected 15-min parameters not found in columns"

    # If time_delta is 15, hourly params are also fetched and interpolated
    if OpenMeteo.params_hourly:  # Default list of hourly parameters
        # Check for at least one common hourly parameter (which would be interpolated)
        assert any(
            param in data.columns
            for param in [
                OpenMeteo1hourParam.pressure_level_0.value,
                OpenMeteo1hourParam.wind_speed_10.value,
            ]
        ), "Expected hourly (interpolated) parameters not found in columns"


def test_openmeteo_load_data_with_gfs_model(openmeteo_test_data):
    latitude, longitude, openmeteo_instance = openmeteo_test_data

    start_dt = datetime.now(timezone.utc) - timedelta(hours=12)
    end_dt = datetime.now(timezone.utc) + timedelta(hours=12)

    data = openmeteo_instance.load_data(
        latitudes=latitude,
        longitudes=longitude,
        time_delta=15,
        start_time=start_dt,
        end_time=end_dt,
        model=OpenMeteoModelType.gfs_global,
    )
    assert isinstance(data, pd.DataFrame)
    assert not data.empty
    assert "datetime" in data.columns

