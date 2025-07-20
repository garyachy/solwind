import requests
import pandas as pd
import urllib.parse


class VisualCrossingAPI:
    def __init__(
        self,
        api_key,
        base_url="https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline",
        timelinemulti_url="https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timelinemulti",
    ):
        """
        Initializes the VisualCrossingAPI with an API key and base URLs.

        Args:
            api_key (str): The API key for accessing the Visual Crossing API.
            base_url (str, optional): The base URL for the Visual Crossing API.
                Defaults to "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline".
            timelinemulti_url (str, optional): The base URL for the timelinemulti endpoint.
                Defaults to "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timelinemulti".
        """
        if not api_key:
            raise ValueError("API_KEY cannot be empty.")
        self.api_key = api_key
        self.base_url = base_url
        self.timelinemulti_url = timelinemulti_url

    def _fetch_forecast_data(
        self,
        locations,
        start_datetime=None,
        end_datetime=None,
        unit_group="metric",
        include="hours,minutes",
    ):
        """
        Fetches raw forecast data from the Visual Crossing API /timelinemulti endpoint.
        """
        locations_str = "|".join([f"{lat},{lon}" for lat, lon in locations])
        params = {
            "key": self.api_key,
            "locations": locations_str,
            "unitGroup": unit_group,
            "contentType": "json",
            "include": include,
        }
        if start_datetime:
            params["datestart"] = start_datetime
        if end_datetime:
            params["dateend"] = end_datetime
        response = requests.get(self.timelinemulti_url, params=params)
        response.raise_for_status()
        return response.json()

    def _parse_forecast_data(self, data):
        """
        Parses the multi-location forecast data into a list of DataFrames.
        """
        def extract_records(loc_result):
            lat = loc_result.get("latitude")
            lon = loc_result.get("longitude")
            records = []
            for day in loc_result.get("days", []):
                day_base = {k: v for k, v in day.items() if k != "hours"}
                hours = day.get("hours", [])
                if hours:
                    for hour in hours:
                        hour_base = {k: v for k, v in hour.items() if k != "minutes"}
                        minutes = hour.get("minutes", [])
                        if minutes:
                            for minute in minutes:
                                merged = {**day_base, **hour_base, **minute}
                                if any(
                                    v is not None and k not in ("datetime", "datetimeEpoch")
                                    for k, v in merged.items()
                                ):
                                    records.append(merged)
                        else:
                            merged = {**day_base, **hour_base}
                            records.append(merged)
                else:
                    records.append(day_base)
            if not records:
                return pd.DataFrame()
            df = pd.DataFrame(records)
            if "datetimeEpoch" in df.columns:
                df["timestamp"] = pd.to_datetime(df["datetimeEpoch"], unit="s")
                cols = ["timestamp"] + [col for col in df.columns if col not in ["timestamp", "datetimeEpoch"]]
                df = df[cols]
            df["latitude"] = lat
            df["longitude"] = lon
            return df

        locations_data = data.get("locations", [])
        return [extract_records(loc_result) for loc_result in locations_data]

    def get_forecast(
        self,
        locations,
        start_datetime=None,
        end_datetime=None,
        unit_group="metric",
        include="hours,minutes",
    ):
        """
        Retrieves weather forecast data from Visual Crossing API for multiple locations using the /timelinemulti endpoint.
        """
        data = self._fetch_forecast_data(
            locations,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            unit_group=unit_group,
            include=include,
        )
        return self._parse_forecast_data(data)

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
