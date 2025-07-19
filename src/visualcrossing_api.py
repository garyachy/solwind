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
        unit_group="metric",
    ):
        """
        Retrieves weather forecast data from Visual Crossing API at minute-level precision if available, otherwise hourly or daily, and returns it as a pandas DataFrame. Each row contains merged day, hour, and minute context.
        """
        if latitude is None or longitude is None:
            raise ValueError("Latitude or longitude cannot be None.")

        if start_datetime and end_datetime:
            url = f"{self.base_url}/{latitude},{longitude}/{start_datetime}/{end_datetime}"
        else:
            url = f"{self.base_url}/{latitude},{longitude}"

        params = {
            "key": self.api_key,
            "unitGroup": unit_group,
            "lang": "en",
            "contentType": "json",
            "include": "hours,minutes",
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        records = []
        if "days" in data:
            for day in data["days"]:
                day_base = {k: v for k, v in day.items() if k not in ["hours"]}
                if "hours" in day and day["hours"]:
                    for hour in day["hours"]:
                        hour_base = {k: v for k, v in hour.items() if k not in ["minutes"]}
                        if "minutes" in hour and hour["minutes"]:
                            for minute in hour["minutes"]:
                                # Merge: day < hour < minute (minute overrides hour, hour overrides day)
                                merged = {**day_base, **hour_base, **minute}
                                records.append(merged)
                        else:
                            # No minutes, merge day and hour
                            merged = {**day_base, **hour_base}
                            records.append(merged)
                else:
                    # No hours, just use day
                    records.append(day_base)

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)

        # Use the most granular datetimeEpoch for timestamp
        if "datetimeEpoch" in df.columns:
            df["timestamp"] = pd.to_datetime(df["datetimeEpoch"], unit="s")
            cols = ["timestamp"] + [col for col in df.columns if col not in ["timestamp", "datetimeEpoch"]]
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
        Retrieves historical weather data from Visual Crossing API at 15-minute precision and returns it as a pandas DataFrame.

        Args:
            latitude (float): Latitude of the location.
            longitude (float): Longitude of the location.
            start_date (str): Start date for the historical data (YYYY-MM-DD).
            end_date (str): End date for the historical data (YYYY-MM-DD).

        Returns:
            pandas.DataFrame: DataFrame containing the 15-minute historical data,
                              with the first column as 'timestamp' and other columns
                              representing weather parameters.

        Raises:
            ValueError: If latitude, longitude, start_date, or end_date is None.
            requests.exceptions.HTTPError: If the HTTP request returns an error status code.
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
            "include": "hours",
            "aggregateMinutes": 15,
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        all_minutes_data = []
        if "days" in data:
            for day_data in data["days"]:
                if "hours" in day_data:
                    all_minutes_data.extend(day_data["hours"])

        if not all_minutes_data:
            return pd.DataFrame()

        df = pd.DataFrame(all_minutes_data)

        if "datetimeEpoch" in df.columns:
            df["timestamp"] = pd.to_datetime(df["datetimeEpoch"], unit="s")
            cols = ["timestamp"] + [
                col for col in df.columns if col not in ["timestamp", "datetimeEpoch"]
            ]
            df = df[cols]

        return df
