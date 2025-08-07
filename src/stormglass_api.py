import requests
import pandas as pd
import datetime as dt
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


class StormglassAPI:
    def __init__(self, api_key):
        """
        Initializes the StormglassAPI with API key.

        Args:
            api_key (str): The API key for accessing the Stormglass API.
        """
        if not api_key:
            raise ValueError("API key cannot be empty.")
        self.api_key = api_key
        self.base_url = "https://api.stormglass.io"

    def get_forecast(
        self,
        locations,
        parameters=None,
        start_datetime=None,
        end_datetime=None,
        interval=None,
    ):
        """
        Retrieves weather forecast data from Stormglass API with hourly resolution.

        Args:
            locations (list): List of (lat, lon) tuples.
            parameters (list, optional): List of weather parameters. Defaults to ["airTemperature"].
            start_datetime (datetime, optional): Start datetime for the forecast.
            end_datetime (datetime, optional): End datetime for the forecast.
            interval (str, optional): Data interval. Note: Stormglass API only supports hourly data.

        Returns:
            list: List of DataFrames, one for each location.
        """
        if not isinstance(locations, list) or not locations:
            raise ValueError(
                "locations must be a non-empty list of (latitude, longitude) tuples."
            )

        if parameters is None or parameters == []:
            parameters = ["airTemperature"]  # Temperature as default

        if start_datetime is None:
            start_datetime = dt.datetime.now(dt.timezone.utc).replace(
                minute=0, second=0, microsecond=0
            )

        if end_datetime is None:
            end_datetime = start_datetime + dt.timedelta(hours=24)

        # Stormglass API only supports hourly data
        if interval is not None and interval != "1h":
            logger.warning(f"Stormglass API only supports hourly data. Ignoring interval: {interval}")

        results = []
        for lat, lon in locations:
            if lat is None or lon is None:
                raise ValueError("Latitude or longitude cannot be None.")

            try:
                # Request time series data
                df = self._get_time_series_data(
                    lat=lat,
                    lon=lon,
                    parameters=parameters,
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                    interval=interval,
                )

                if not df.empty:
                    # Add location information
                    df["latitude"] = lat
                    df["longitude"] = lon
                    results.append(df)
                else:
                    logger.warning(f"No data received for location ({lat}, {lon})")
                    # Return empty DataFrame with location info for consistency
                    empty_df = pd.DataFrame(
                        {
                            "datetime": pd.date_range(
                                start=start_datetime, end=end_datetime, freq="1h"
                            ),
                            "latitude": lat,
                            "longitude": lon,
                        }
                    )
                    results.append(empty_df)

            except Exception as e:
                logger.error(
                    f"Error retrieving data for location ({lat}, {lon}): {str(e)}"
                )
                # Re-raise the exception instead of returning empty data
                raise

        return results

    def get_high_resolution_forecast(
        self,
        locations,
        parameters=None,
        start_datetime=None,
        end_datetime=None,
    ):
        """
        Retrieves weather forecast data from Stormglass API with hourly resolution.
        Note: Stormglass API only provides hourly data, not 15-minute intervals.

        Args:
            locations (list): List of (lat, lon) tuples.
            parameters (list, optional): List of weather parameters. Defaults to ["airTemperature"].
            start_datetime (datetime, optional): Start datetime for the forecast.
            end_datetime (datetime, optional): End datetime for the forecast.

        Returns:
            list: List of DataFrames, one for each location.
        """
        return self.get_forecast(
            locations=locations,
            parameters=parameters,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            interval="1h",
        )

    def get_standard_forecast(
        self,
        locations,
        parameters=None,
        start_datetime=None,
        end_datetime=None,
    ):
        """
        Retrieves standard weather forecast data from Stormglass API with 1-hour intervals.

        Args:
            locations (list): List of (lat, lon) tuples.
            parameters (list, optional): List of weather parameters. Defaults to ["airTemperature"].
            start_datetime (datetime, optional): Start datetime for the forecast.
            end_datetime (datetime, optional): End datetime for the forecast.

        Returns:
            list: List of DataFrames, one for each location.
        """
        return self.get_forecast(
            locations=locations,
            parameters=parameters,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            interval="1h",
        )

    def _get_time_series_data(
        self,
        lat,
        lon,
        parameters,
        start_datetime,
        end_datetime,
        interval,
    ):
        """
        Internal method to retrieve time series data from Stormglass API.

        Args:
            lat (float): Latitude.
            lon (float): Longitude.
            parameters (list): List of weather parameters.
            start_datetime (datetime): Start datetime.
            end_datetime (datetime): End datetime.
            interval (str): Data interval.

        Returns:
            pd.DataFrame: DataFrame with weather data.
        """
        # Prepare request parameters
        params = {
            "lat": lat,
            "lng": lon,
            "params": ",".join(parameters),
            "source": "sg",  # Use Stormglass source
        }

        # Add time range if specified
        if start_datetime:
            params["start"] = start_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        if end_datetime:
            params["end"] = end_datetime.strftime("%Y-%m-%dT%H:%M:%S")

        headers = {"Authorization": self.api_key}

        try:
            response = requests.get(
                f"{self.base_url}/v1/weather/point", params=params, headers=headers, timeout=30
            )
            response.raise_for_status()

            data = response.json()

            if "hours" not in data or not data["hours"]:
                logger.warning("No hourly data received from Stormglass API")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(data["hours"])

            # Convert timestamp to datetime
            if "time" in df.columns:
                df["datetime"] = pd.to_datetime(df["time"])
                df = df.drop("time", axis=1)

            # Flatten nested parameter data
            df = self._flatten_parameter_data(df, parameters)

            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error processing response: {str(e)}")
            raise

    def _convert_parameters(self, parameters):
        """
        Convert standard parameter names to Stormglass API format.
        Note: Stormglass API expects the original parameter names, not converted ones.

        Available parameters (based on Stormglass API documentation):
        - airTemperature: Air temperature in Celsius
        - windSpeed: Wind speed in m/s
        - windDirection: Wind direction in degrees
        - pressure: Atmospheric pressure in hPa
        - precipitation: Precipitation in mm
        - humidity: Relative humidity in %
        - cloudCover: Cloud cover in %
        - visibility: Visibility in meters
        - gust: Wind gust in m/s
        - currentDirection: Current direction in degrees
        - currentSpeed: Current speed in m/s
        - swellDirection: Swell direction in degrees
        - swellHeight: Swell height in meters
        - swellPeriod: Swell period in seconds
        - waterTemperature: Water temperature in Celsius
        - waveDirection: Wave direction in degrees
        - waveHeight: Wave height in meters
        - wavePeriod: Wave period in seconds
        - windWaveDirection: Wind wave direction in degrees
        - windWaveHeight: Wind wave height in meters
        - windWavePeriod: Wind wave period in seconds
        - seaLevel: Sea level in meters

        Args:
            parameters (list): List of standard parameter names.

        Returns:
            list: List of Stormglass parameter names (same as input).
        """
        # Stormglass API expects the original parameter names
        # No conversion needed - return parameters as-is
        return parameters

    def _flatten_parameter_data(self, df, parameters):
        """
        Flatten nested parameter data in the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame with nested parameter data.
            parameters (list): List of parameter names.

        Returns:
            pd.DataFrame: Flattened DataFrame.
        """
        # Stormglass API returns data in format like:
        # {
        #   "time": "2024-01-01T00:00:00+00:00",
        #   "airTemperature": [
        #     {"source": "sg", "value": 15.2},
        #     {"source": "noaa", "value": 15.2}
        #   ]
        # }

        flattened_df = df.copy()

        for param in parameters:
            if param in flattened_df.columns:
                # Extract the 'sg' value from nested structure
                if isinstance(flattened_df[param].iloc[0], list):
                    # Handle array format: [{"source": "sg", "value": 15.2}, ...]
                    flattened_df[param] = flattened_df[param].apply(
                        lambda x: next((item["value"] for item in x if item["source"] == "sg"), None) if isinstance(x, list) else x
                    )
                elif isinstance(flattened_df[param].iloc[0], dict):
                    # Handle dict format: {"sg": 15.2} (fallback)
                    flattened_df[param] = flattened_df[param].apply(
                        lambda x: x.get("sg") if isinstance(x, dict) else x
                    )

        return flattened_df
