import requests


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
        aggregate_minutes=None,
    ):
        """
        Retrieves weather forecast data from Visual Crossing API.

        Args:
            latitude (float): Latitude of the location.
            longitude (float): Longitude of the location.
            start_datetime (str, optional): Start datetime for the forecast. Defaults to None.
            end_datetime (str, optional): End datetime for the forecast. Defaults to None.
            aggregate_minutes (int, optional): Aggregate minutes for the forecast. Defaults to None.

        Returns:
            dict: JSON response from the Visual Crossing API.

        Raises:
            ValueError: If latitude or longitude is None.
            requests.exceptions.HTTPError: If the HTTP request returns an error status code.
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
        }

        if aggregate_minutes:
            params["aggregateMinutes"] = aggregate_minutes
            params["include"] = "hours"
        else:
            params["include"] = "hours"

        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
