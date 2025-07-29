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
        Fetches raw forecast data from the Visual Crossing API.
        Uses /timeline for a single location, /timelinemulti for multiple locations.
        """
        if not isinstance(locations, list) or not locations:
            raise ValueError(
                "locations must be a non-empty list of (latitude, longitude) tuples."
            )
        params = {
            "key": self.api_key,
            "unitGroup": unit_group,
            "contentType": "json",
            "include": include,
        }
        if len(locations) == 1:
            lat, lon = locations[0]
            if lat is None or lon is None:
                raise ValueError(
                    "Latitude or longitude cannot be None for single-location forecast."
                )
            params["lang"] = "en"
            url = f"{self.base_url}/{lat},{lon}"
            if start_datetime and end_datetime:
                url = f"{self.base_url}/{lat},{lon}/{start_datetime}/{end_datetime}"
        else:
            url = self.timelinemulti_url
            params["locations"] = "|".join([f"{lat},{lon}" for lat, lon in locations])
            if start_datetime:
                params["datestart"] = start_datetime
            if end_datetime:
                params["dateend"] = end_datetime
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _parse_forecast_data(self, data):
        """
        Parses forecast data from either /timelinemulti (multi-location) or /timeline (single-location) endpoint.
        Returns a list of DataFrames for multi-location, or a single DataFrame for single-location.
        """
        # Multi-location format: {"locations": [ ... ]}
        if "locations" in data:
            locations_data = data.get("locations", [])
            return [self._extract_records(loc_result) for loc_result in locations_data]
        # Single-location format: {"days": [ ... ]}
        elif "days" in data:
            return self._extract_records(data)
        else:
            return pd.DataFrame()

    def _extract_records(self, loc_result):
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
            cols = ["timestamp"] + [
                col for col in df.columns if col not in ["timestamp", "datetimeEpoch"]
            ]
            df = df[cols]
        if lat is not None and lon is not None:
            df["latitude"] = lat
            df["longitude"] = lon
        return df

    def _validate_and_fill_timestamps(
        self, df, start_datetime, end_datetime, freq="15min"
    ):
        """
        Validates that the DataFrame's 'timestamp' column contains all expected intervals.
        Raises ValueError if any expected timestamp is missing.
        """
        if df.empty or "timestamp" not in df.columns:
            return df  # Nothing to validate
        # Parse start/end as pd.Timestamp if needed
        if isinstance(start_datetime, str):
            start = pd.to_datetime(start_datetime)
        else:
            start = start_datetime
        if isinstance(end_datetime, str):
            end = pd.to_datetime(end_datetime)
        else:
            end = end_datetime
        expected_timestamps = pd.date_range(start=start, end=end, freq=freq)
        timestamps = pd.Series(df["timestamp"]).sort_values().reset_index(drop=True)
        timestamps_set = set(timestamps)
        missing = [ts for ts in expected_timestamps if ts not in timestamps_set]
        if missing:
            raise ValueError(
                f"Missing expected {freq} timestamps between {start} and {end}: {missing}\nReturned timestamps: {list(timestamps)}"
            )
        return df

    def get_forecast(
        self,
        locations,
        start_datetime=None,
        end_datetime=None,
        unit_group="metric",
    ):
        """
        Retrieves weather forecast data from Visual Crossing API.
        - If locations contains one (lat, lon) tuple, uses /timeline and returns a single DataFrame.
        - If locations contains multiple tuples, uses /timelinemulti and returns a list of DataFrames.
        Now also validates that all expected 15-min timestamps are present.
        """
        data = self._fetch_forecast_data(
            locations,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            unit_group=unit_group,
            include="hours,minutes",
        )
        parsed = self._parse_forecast_data(data)
        # Only validate if both start and end datetimes are provided
        if start_datetime and end_datetime:
            if isinstance(parsed, list):
                return [
                    self._validate_and_fill_timestamps(df, start_datetime, end_datetime)
                    for df in parsed
                ]
            else:
                return self._validate_and_fill_timestamps(
                    parsed, start_datetime, end_datetime
                )
        return parsed

    def get_historical_data(
        self,
        latitude,
        longitude,
        start_date,
        end_date,
    ):
        """
        Retrieves historical weather data from Visual Crossing API at hourly precision and returns it as a pandas DataFrame.

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
