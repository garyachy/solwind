import requests
import pandas as pd


class VisualCrossingAPI:
    def __init__(
        self,
        api_key,
        base_url="https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline",
    ):
        """
        Initializes the VisualCrossingAPI with an API key and base URL.

        Args:
            api_key (str): The API key for accessing the Visual Crossing API.
            base_url (str, optional): The base URL for the Visual Crossing API.
                Defaults to "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline".
        """
        if not api_key:
            raise ValueError("API_KEY cannot be empty.")
        self.api_key = api_key
        self.base_url = base_url

    def get_forecast(
        self,
        latitude,
        longitude,
        start_datetime=None,
        end_datetime=None,
    ):
        """
        Retrieves weather forecast data from Visual Crossing API and returns it as a pandas DataFrame.

        Args:
            latitude (float): Latitude of the location.
            longitude (float): Longitude of the location.
            start_datetime (str, optional): Start datetime for the forecast. Defaults to None.
            end_datetime (str, optional): End datetime for the forecast. Defaults to None.

        Returns:
            pandas.DataFrame: DataFrame containing the hourly forecast data,
                              with the first column as 'timestamp' and other columns
                              representing weather parameters.

        Raises:
            ValueError: If latitude or longitude is None.
            requests.exceptions.HTTPError: If the HTTP request returns an error status code.
            KeyError: If the 'days' or 'hours' section is not found in the API response.
        """
        if latitude is None or longitude is None:
            raise ValueError("Latitude or longitude cannot be None.")

        if start_datetime and end_datetime:
            url = f"{self.base_url}/{latitude},{longitude}/{start_datetime}/{end_datetime}"
        else:
            url = f"{self.base_url}/{latitude},{longitude}"

        params = {
            "key": self.api_key,
            "unitGroup": "metric",
            "lang": "en",
            "contentType": "json",
            "include": "hours",
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        all_hours_data = []
        if "days" in data:
            for day_data in data["days"]:
                if "hours" in day_data:
                    all_hours_data.extend(day_data["hours"])

        if not all_hours_data:
            if "hours" in data:
                all_hours_data = data["hours"]
            elif "days" in data and data["days"] and "hours" in data["days"][0]:
                all_hours_data = data["days"][0]["hours"]
            else:
                pass

        if not all_hours_data:
            return pd.DataFrame()

        df = pd.DataFrame(all_hours_data)

        if "datetimeEpoch" in df.columns:
            df["timestamp"] = pd.to_datetime(df["datetimeEpoch"], unit="s")
            cols = ["timestamp"] + [
                col for col in df.columns if col not in ["timestamp", "datetimeEpoch"]
            ]
            df = df[cols]

        return df

    def get_historical_data(
        self,
        latitude,
        longitude,
        start_date,
        end_date,
    ):
        """
        Retrieves historical weather data from Visual Crossing API and returns it as a pandas DataFrame.

        Args:
            latitude (float): Latitude of the location.
            longitude (float): Longitude of the location.
            start_date (str): Start date for the historical data (YYYY-MM-DD).
            end_date (str): End date for the historical data (YYYY-MM-DD).

        Returns:
            pandas.DataFrame: DataFrame containing the hourly historical data,
                              with the first column as 'timestamp' and other columns
                              representing weather parameters.

        Raises:
            ValueError: If latitude, longitude, start_date, or end_date is None.
            requests.exceptions.HTTPError: If the HTTP request returns an error status code.
            KeyError: If the 'days' or 'hours' section is not found in the API response.
        """
        if latitude is None or longitude is None:
            raise ValueError("Latitude or longitude cannot be None.")
        if not start_date or not end_date:
            raise ValueError(
                "Start date and end date must be provided for historical data."
            )

        url = f"{self.base_url}/{latitude},{longitude}/{start_date}/{end_date}"

        params = {
            "key": self.api_key,
            "unitGroup": "metric",
            "lang": "en",
            "contentType": "json",
            "include": "hours",  # Ensure hourly data is included
        }

        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()

        all_hours_data = []
        if "days" in data:
            for day_data in data["days"]:
                if "hours" in day_data:
                    all_hours_data.extend(day_data["hours"])

        if (
            not all_hours_data
        ):  # Fallback for single day requests that might not have 'days'
            if "hours" in data:
                all_hours_data = data["hours"]
            elif "days" in data and data["days"] and "hours" in data["days"][0]:
                all_hours_data = data["days"][0]["hours"]
            else:  # No hourly data found
                pass

        if not all_hours_data:
            return pd.DataFrame()  # Return empty DataFrame if no hourly data

        df = pd.DataFrame(all_hours_data)

        if "datetimeEpoch" in df.columns:
            df["timestamp"] = pd.to_datetime(df["datetimeEpoch"], unit="s")
            # Reorder columns to have 'timestamp' first
            cols = ["timestamp"] + [
                col for col in df.columns if col not in ["timestamp", "datetimeEpoch"]
            ]
            df = df[cols]

        return df
