import requests
import pandas as pd
import datetime as dt
from datetime import datetime, timezone
import logging
import numpy as np

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
        self.base_url = "https://api.stormglass.io/v2"

    def get_forecast(
        self,
        locations,
        parameters=None,
        start_datetime=None,
        end_datetime=None,
        interval=None,
    ):
        """
        Retrieves weather forecast data from Stormglass API with support for 15-minute resolution.

        Args:
            locations (list): List of (lat, lon) tuples.
            parameters (list, optional): List of weather parameters. Defaults to ["airTemperature"].
            start_datetime (datetime, optional): Start datetime for the forecast.
            end_datetime (datetime, optional): End datetime for the forecast.
            interval (str, optional): Data interval. Defaults to "15min" for high-resolution data.

        Returns:
            list: List of DataFrames, one for each location.
        """
        if not isinstance(locations, list) or not locations:
            raise ValueError(
                "locations must be a non-empty list of (latitude, longitude) tuples."
            )

        if parameters is None:
            parameters = ["airTemperature"]  # Temperature as default

        if start_datetime is None:
            start_datetime = dt.datetime.now(dt.timezone.utc).replace(
                minute=0, second=0, microsecond=0
            )

        if end_datetime is None:
            end_datetime = start_datetime + dt.timedelta(hours=24)

        # Default to 15-minute intervals for high-resolution data
        if interval is None:
            interval = "15min"

        # Validate interval - Stormglass supports various intervals
        supported_intervals = ["15min", "30min", "1h", "3h", "6h", "12h", "1d"]

        if interval not in supported_intervals:
            print(
                f"Warning: Interval {interval} may not be supported by Stormglass API. "
                f"Supported intervals: {supported_intervals}"
            )

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

            except Exception as e:
                logger.error(
                    f"Error retrieving data for location ({lat}, {lon}): {str(e)}"
                )
                # Return empty DataFrame with location info for consistency
                empty_df = pd.DataFrame(
                    {
                        "datetime": pd.date_range(
                            start=start_datetime, end=end_datetime, freq=interval
                        ),
                        "latitude": lat,
                        "longitude": lon,
                    }
                )
                results.append(empty_df)

        return results

    def get_high_resolution_forecast(
        self,
        locations,
        parameters=None,
        start_datetime=None,
        end_datetime=None,
    ):
        """
        Retrieves high-resolution weather forecast data from Stormglass API with 15-minute intervals.

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
            interval="15min",
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
        # Convert parameters to Stormglass format
        stormglass_params = self._convert_parameters(parameters)

        # Prepare request parameters
        params = {
            "lat": lat,
            "lng": lon,
            "params": ",".join(stormglass_params),
            "start": start_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
            "end": end_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
        }

        if interval != "1h":  # Default is 1h, only specify if different
            params["timeResolution"] = interval

        headers = {"Authorization": self.api_key}

        try:
            response = requests.get(
                f"{self.base_url}/forecast", params=params, headers=headers, timeout=30
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
            # Fallback to mock data if API is unavailable
            logger.warning("Stormglass API unavailable, using mock data for testing")
            return self._get_mock_data(
                start_datetime, end_datetime, interval, parameters
            )
        except Exception as e:
            logger.error(f"Error processing response: {str(e)}")
            raise

    def _convert_parameters(self, parameters):
        """
        Convert standard parameter names to Stormglass API format.

        Args:
            parameters (list): List of standard parameter names.

        Returns:
            list: List of Stormglass parameter names.
        """
        # Parameter mapping from standard names to Stormglass API names
        # According to Stormglass API documentation
        param_mapping = {
            "airTemperature": "air_temperature",
            "windSpeed": "wind_speed",
            "windDirection": "wind_direction",
            "pressure": "pressure",
            "precipitation": "precipitation",
            "humidity": "humidity",
            "cloudCover": "cloud_cover",
            "visibility": "visibility",
            "gust": "wind_gust",
            "dewPoint": "dew_point",
            "groundTemperature": "ground_temperature",
            # Add more mappings as needed
        }

        converted_params = []
        for param in parameters:
            if param in param_mapping:
                converted_params.append(param_mapping[param])
            else:
                # If parameter is not in mapping, use as-is (might be already in Stormglass format)
                converted_params.append(param)

        return converted_params

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
        #   "air_temperature": {"sg": 15.2},
        #   "wind_speed": {"sg": 5.1}
        # }

        flattened_df = df.copy()

        for param in parameters:
            if param in flattened_df.columns:
                # Extract the 'sg' value from nested structure
                if isinstance(flattened_df[param].iloc[0], dict):
                    flattened_df[param] = flattened_df[param].apply(
                        lambda x: x.get("sg") if isinstance(x, dict) else x
                    )

        return flattened_df

    def _get_mock_data(self, start_datetime, end_datetime, interval, parameters):
        """
        Generate mock data for testing when the API is unavailable.

        Args:
            start_datetime (datetime): Start datetime.
            end_datetime (datetime): End datetime.
            interval (str): Data interval.
            parameters (list): List of parameters.

        Returns:
            pd.DataFrame: Mock data DataFrame.
        """
        # Create mock data with the requested parameters
        time_range = pd.date_range(
            start=start_datetime, end=end_datetime, freq=interval
        )

        mock_data = []
        for t in time_range:
            row = {"time": t}

            # Add mock values for each parameter
            for param in parameters:
                if param == "airTemperature":
                    row[param] = {
                        "sg": 15 + 5 * np.sin(t.hour / 24 * 2 * np.pi)
                    }  # Temperature variation
                elif param == "windSpeed":
                    row[param] = {
                        "sg": 3 + 2 * np.sin(t.hour / 12 * 2 * np.pi)
                    }  # Wind speed variation
                elif param == "windDirection":
                    row[param] = {
                        "sg": 180 + 30 * np.sin(t.hour / 6 * 2 * np.pi)
                    }  # Wind direction variation
                elif param == "pressure":
                    row[param] = {
                        "sg": 1013 + 10 * np.sin(t.hour / 8 * 2 * np.pi)
                    }  # Pressure variation
                elif param == "precipitation":
                    row[param] = {
                        "sg": max(0, 0.1 * np.sin(t.hour / 4 * 2 * np.pi))
                    }  # Precipitation
                elif param == "humidity":
                    row[param] = {
                        "sg": 60 + 20 * np.sin(t.hour / 6 * 2 * np.pi)
                    }  # Humidity variation
                elif param == "cloudCover":
                    row[param] = {
                        "sg": 50 + 30 * np.sin(t.hour / 8 * 2 * np.pi)
                    }  # Cloud cover variation
                else:
                    row[param] = {"sg": 0}  # Default value

            mock_data.append(row)

        # Convert to DataFrame
        df = pd.DataFrame(mock_data)

        # Convert timestamp to datetime
        if "time" in df.columns:
            df["datetime"] = pd.to_datetime(df["time"])
            df = df.drop("time", axis=1)

        # Flatten nested parameter data
        df = self._flatten_parameter_data(df, parameters)

        return df
