from datetime import datetime, timezone
import datetime as dt
import meteomatics.api as api
from config import load_config


def test_meteomatics_access():
    # Load configuration
    config = load_config()
    meteomatics_config = config.get("Meteomatics", {})
    location_config = config.get("Location", {})

    # Specify credentials for the Meteomatics API
    username = meteomatics_config.get("username")
    password = meteomatics_config.get("password")

    if not username or not password:
        raise ValueError(
            "Username or password not found in config.json for Meteomatics"
        )

    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")

    if latitude is None or longitude is None:
        raise ValueError("Latitude or longitude not found in config.json")

    # Prepare request parameters
    coordinates = [(latitude, longitude)]
    parameters = ["t_2m:C"]  # Temperature
    model = "mix"  # Weather model (e.g., 'mix' for combined models)
    startdate = dt.datetime.utcnow().replace(
        minute=0, second=0, microsecond=0
    )  # Current time, rounded to the hour
    enddate = startdate  # Single time point for testing
    interval = dt.timedelta(hours=1)  # Data with a one-hour interval

    try:
        # Request time series data for all stations
        df = api.query_time_series(
            coordinate_list=coordinates,
            startdate=startdate,
            enddate=enddate,
            interval=interval,
            parameters=parameters,
            username=username,
            password=password,
            model=model,
        )
        assert not df.empty, "Dataframe is empty, no data received"
        print("Weather data successfully received for all stations:")
        print(df)

    except Exception as e:
        print(e)
        assert False, f"An error occurred: {e}"
