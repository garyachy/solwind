import meteomatics.api as api
import pandas as pd
import datetime as dt
from datetime import datetime, timezone


class MeteomaticsAPI:
    def __init__(self, username, password):
        """
        Initializes the MeteomaticsAPI with username and password.

        Args:
            username (str): The username for accessing the Meteomatics API.
            password (str): The password for accessing the Meteomatics API.
        """
        if not username or not password:
            raise ValueError("Username and password cannot be empty.")
        self.username = username
        self.password = password

    def get_forecast(
        self,
        locations,
        parameters=None,
        start_datetime=None,
        end_datetime=None,
        model="mix",
        interval=None,
    ):
        """
        Retrieves weather forecast data from Meteomatics API.

        Args:
            locations (list): List of (lat, lon) tuples.
            parameters (list, optional): List of weather parameters. Defaults to ["t_2m:C"].
            start_datetime (datetime, optional): Start datetime for the forecast.
            end_datetime (datetime, optional): End datetime for the forecast.
            model (str, optional): Weather model to use. Defaults to "mix".
            interval (timedelta, optional): Data interval. Defaults to 1 hour.

        Returns:
            list: List of DataFrames, one for each location.
        """
        if not isinstance(locations, list) or not locations:
            raise ValueError(
                "locations must be a non-empty list of (latitude, longitude) tuples."
            )

        if parameters is None:
            parameters = ["t_2m:C"]  # Temperature as default

        if start_datetime is None:
            start_datetime = dt.datetime.now(dt.timezone.utc).replace(
                minute=0, second=0, microsecond=0
            )

        if end_datetime is None:
            end_datetime = start_datetime

        if interval is None:
            interval = dt.timedelta(hours=1)

        results = []
        for lat, lon in locations:
            if lat is None or lon is None:
                raise ValueError("Latitude or longitude cannot be None.")

            coordinates = [(lat, lon)]

            try:
                # Request time series data
                df = api.query_time_series(
                    coordinate_list=coordinates,
                    startdate=start_datetime,
                    enddate=end_datetime,
                    interval=interval,
                    parameters=parameters,
                    username=self.username,
                    password=self.password,
                    model=model,
                )

                if not df.empty:
                    # Add location information
                    df["latitude"] = lat
                    df["longitude"] = lon
                    
                    # Handle datetime conversion - Meteomatics returns data with MultiIndex
                    # The MultiIndex contains (lat, lon, validdate) where validdate is the datetime
                    if isinstance(df.index, pd.MultiIndex):
                        # Extract datetime from the third level of the MultiIndex
                        datetime_level = df.index.get_level_values(2)
                        df["datetime"] = pd.to_datetime(datetime_level)
                        # Reset index to remove the MultiIndex
                        df = df.reset_index(drop=True)
                    elif "validdate" in df.columns:
                        # Fallback: if validdate column exists, use it
                        df["datetime"] = pd.to_datetime(df["validdate"])
                        df = df.drop(columns=["validdate"])
                    elif "datetime" not in df.columns and not df.empty:
                        # If no datetime information available, create based on request parameters
                        datetime_range = pd.date_range(
                            start=start_datetime,
                            end=end_datetime,
                            freq=interval,
                            tz=timezone.utc
                        )
                        df["datetime"] = datetime_range[:len(df)]

                    # Ensure datetime column is timezone-aware and in UTC
                    if "datetime" in df.columns:
                        if df["datetime"].dt.tz is None:
                            df["datetime"] = df["datetime"].dt.tz_localize(timezone.utc)
                        elif df["datetime"].dt.tz != timezone.utc:
                            df["datetime"] = df["datetime"].dt.tz_convert(timezone.utc)

                results.append(df)
            except Exception as e:
                print(f"Error fetching data for location ({lat}, {lon}): {e}")
                # Return empty DataFrame for failed locations
                results.append(pd.DataFrame())

        return results 