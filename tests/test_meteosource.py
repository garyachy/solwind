from pymeteosource.api import Meteosource
from config import load_config

# Load configuration
config = load_config()
meteosource_config = config.get("Meteosource", {})
location_config = config.get("Location", {})
API_KEY = meteosource_config.get("api_key")
TIER = "free"  # Assuming TIER remains constant or you can add it to config.json as well
latitude = location_config.get("latitude")
longitude = location_config.get("longitude")


def get_meteosource():
    if not API_KEY:
        raise ValueError("API_KEY not found in config.json for Meteosource")
    if latitude is None or longitude is None:
        raise ValueError("Latitude or longitude not found in config.json")
    return Meteosource(API_KEY, TIER)


def run_forecast_test(
    latitude, longitude, section, attr, to_df=False, print_label=None
):
    meteosource = get_meteosource()
    try:
        forecast = meteosource.get_point_forecast(
            lat=latitude,
            lon=longitude,
            sections=[section],
            lang="en",
            units="metric",
        )
        assert hasattr(
            forecast, attr
        ), f"{section} forecast data not received for station {latitude}"
        data = getattr(forecast, attr)
        if to_df:
            df = data.to_pandas()
            assert (
                not df.empty
            ), f"Data is empty, {section} forecast not received for station {latitude}"
            if print_label:
                print(f"{print_label} [{latitude}]")
                print(df)
        else:
            assert (
                data is not None
            ), f"{section.capitalize()} forecast is empty for station {latitude}"
            if print_label:
                print(f"{print_label} [{latitude}]")
                print(data)
    except Exception as e:
        print(e)
        assert False, f"An error occurred for station {latitude}: {e}"


def test_meteosource_daily_forecast():
    run_forecast_test(
        latitude=latitude,
        longitude=longitude,
        section="daily",
        attr="daily",
        to_df=True,
        print_label="Daily weather data as DataFrame:",
    )


def test_meteosource_current_forecast():

    run_forecast_test(
        latitude=latitude,
        longitude=longitude,
        section="current",
        attr="current",
        to_df=False,
        print_label="Current weather data:",
    )


def test_meteosource_hourly_forecast_full_df():

    run_forecast_test(
        latitude=latitude,
        longitude=longitude,
        section="hourly",
        attr="hourly",
        to_df=True,
        print_label="Full hourly forecast DataFrame:",
    )
