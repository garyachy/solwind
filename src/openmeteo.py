from enum import Enum
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests_cache
import openmeteo_requests
from retry_requests import retry


class OpenMeteoModelType(str, Enum):
    best_match = ("best_match",)
    gfs_global = ("gfs_global",)
    icon_eu = ("icon_eu",)
    icon_d2 = ("icon_d2",)
    meteofrance_arpege_europe = ("meteofrance_arpege_europe",)
    knmi_harmonie_arome_europe = "knmi_harmonie_arome_europe"


class OpenMeteo15mParam(str, Enum):

    temperature_air_2 = "temperature_2m"
    humidity = "relative_humidity_2m"
    dew_point = "dew_point_2m"
    temperature_apparent = "apparent_temperature"
    precipitation = "precipitation"
    rain = "rain"
    snowfall = "snowfall"
    snowfall_height = "snowfall_height"
    freezing_level_height = "freezing_level_height"
    sunshine = "sunshine_duration"
    weather_code = "weather_code"
    wind_speed_10 = "wind_speed_10m"
    wind_speed_80 = "wind_speed_80m"
    wind_direction_10 = "wind_direction_10m"
    wind_direction_80 = "wind_direction_80m"
    wind_gusts_10 = "wind_gusts_10m"
    visibility = "visibility"
    cape = "cape"
    # lightning_potential = ("lightning_potential")
    is_day = "is_day"
    gti = "global_tilted_irradiance"
    ghi = "shortwave_radiation"
    dni = "direct_normal_irradiance"
    dr = "direct_radiation"
    dfr = "diffuse_radiation"
    tr = "terrestrial_radiation"


class OpenMeteo1hourParam(str, Enum):

    humidity = "relative_humidity_2m"
    dew_point = "dew_point_2m"
    temperature_apparent = "apparent_temperature"
    # precipitation_probability = ("precipitation_probability")
    precipitation = "precipitation"
    rain = "rain"
    showers = "showers"
    snowfall = "snowfall"
    snow_depth = "snow_depth"
    weather_code = "weather_code"
    pressure_level_0 = "pressure_msl"
    pressure_surface = "surface_pressure"
    cloud_cover = "cloud_cover"
    cloud_cover_low = "cloud_cover_low"
    cloud_cover_mid = "cloud_cover_mid"
    cloud_cover_high = "cloud_cover_high"
    visibility = "visibility"
    wind_speed_10 = "wind_speed_10m"
    wind_speed_80 = "wind_speed_80m"
    wind_speed_120 = "wind_speed_120m"
    wind_speed_180 = "wind_speed_180m"
    wind_direction_10 = "wind_direction_10m"
    wind_direction_80 = "wind_direction_80m"
    wind_direction_120 = "wind_direction_120m"
    wind_direction_180 = "wind_direction_180m"
    wind_gusts_10 = "wind_gusts_10m"
    temperature_air_2 = "temperature_2m"
    temperature_air_80 = "temperature_80m"
    temperature_air_120 = "temperature_120m"
    temperature_air_180 = "temperature_180m"

    temperature_soil_0 = "soil_temperature_0cm"
    # temperature_soil_6 = ("soil_temperature_6cm")
    # temperature_soil_18 = ("soil_temperature_18cm")
    # temperature_soil_54 = ("soil_temperature_54cm")

    moisture_soil_0 = "soil_moisture_0_to_1cm"
    # moisture_soil_1 = ("soil_moisture_1_to_3cm")
    # moisture_soil_3 = ("soil_moisture_3_to_9cm")
    # moisture_soil_9 = ("soil_moisture_9_to_27cm")
    # moisture_soil_27 = ("soil_moisture_27_to_81cm")

    uv_index = "uv_index"
    uv_index_clear_sky = "uv_index_clear_sky"
    sunshine = "sunshine_duration"
    cape = "cape"

    gti = "global_tilted_irradiance"
    ghi = "shortwave_radiation"
    dni = "direct_normal_irradiance"
    dr = "direct_radiation"
    dfr = "diffuse_radiation"
    tr = "terrestrial_radiation"

    evaporation = "evapotranspiration"
    vapour_pressure_deficit = "vapour_pressure_deficit"


class OpenMeteo:
    models = [
        "best_match",
        "ecmwf_ifs04",
        "ecmwf_ifs025",
        "ecmwf_aifs025",
        "cma_grapes_global",
        "bom_access_global",
        "gfs_seamless",
        "gfs_global",
        "gfs_hrrr",
        "gfs_graphcast025",
        "jma_seamless",
        "jma_msm",
        "jma_gsm",
        "icon_seamless",
        "icon_global",
        "icon_eu",
        "icon_d2",
        "gem_seamless",
        "gem_global",
        "gem_regional",
        "gem_hrdps_continental",
        "meteofrance_seamless",
        "meteofrance_arpege_world",
        "meteofrance_arpege_europe",
        "meteofrance_arome_france",
        "meteofrance_arome_france_hd",
        "arpae_cosmo_seamless",
        "arpae_cosmo_2i",
        "arpae_cosmo_2i_ruc",
        "arpae_cosmo_5m",
        "metno_seamless",
        "metno_nordic",
        "knmi_seamless",
        "knmi_harmonie_arome_europe",
        "knmi_harmonie_arome_netherlands",
        "dmi_seamless",
        "dmi_harmonie_arome_europe",
        "ukmo_seamless",
        "ukmo_global_deterministic_10km",
        "ukmo_uk_deterministic_2km",
    ]

    params_minutely_15 = []
    for param in OpenMeteo15mParam:
        params_minutely_15.append(param.value)

    params_hourly = []
    for param in OpenMeteo1hourParam:
        params_hourly.append(param.value)

    def __init__(self, api_key=None):
        self.api_key = api_key

    def process_response(self, response, params, time_delta):
        minutely_15 = response.Minutely15()
        minutely_15_data = (
            self.extract_data(minutely_15, params["minutely_15"])
            if minutely_15
            else None
        )

        hourly = response.Hourly()
        hourly_data = self.extract_data(hourly, params["hourly"]) if hourly else None

        if time_delta == 15:
            # Interpolate hourly data to 15-minute intervals
            interpolated_hourly_data = self.interpolate_hourly_data(
                hourly_data, minutely_15_data["datetime"]
            )

            # Merge interpolated data into the 15-minute data
            if not interpolated_hourly_data.empty:
                data = minutely_15_data.merge(
                    interpolated_hourly_data,
                    on="datetime",
                    how="left",
                    suffixes=("", "_duplicate"),
                )
                data = data.loc[:, ~data.columns.str.endswith("_duplicate")]
            else:
                data = minutely_15_data
        else:
            data = hourly_data

        return data

    def load_data(
        self,
        stations,
        time_delta,
        params_15m=params_minutely_15,
        params_hourly=params_hourly,
        start_time=datetime.now(timezone.utc) + timedelta(hours=-24),
        end_time=datetime.now(timezone.utc) + timedelta(hours=24),
        model=OpenMeteoModelType.best_match,
        merge=True,
    ):
        try:
            # Make the API call with the given parameters
            latitudes = ",".join([str(station.latitude) for station in stations])
            longitudes = ",".join([str(station.longitude) for station in stations])
            params = {
                "latitude": latitudes,
                "longitude": longitudes,
                "timezone": "auto",
                "start_date": start_time.strftime("%Y-%m-%d"),
                "end_date": end_time.strftime("%Y-%m-%d"),
                "models": [model],
            }

            if self.api_key:
                params["apikey"] = self.api_key

            if time_delta == 15:
                params["minutely_15"] = params_15m
                params["hourly"] = params_hourly
            elif time_delta == 60:
                params["hourly"] = params_hourly

            url = "https://api.open-meteo.com/v1/forecast"

            if (start_time - datetime.now(timezone.utc)).days < -1:
                url = (
                    "https://customer-historical-forecast-api.open-meteo.com/v1/forecast"
                    if self.api_key
                    else "https://historical-forecast-api.open-meteo.com/v1/forecast"
                )
            else:
                url = (
                    "https://customer-api.open-meteo.com/v1/forecast"
                    if self.api_key
                    else "https://api.open-meteo.com/v1/forecast"
                )

            cache_session = requests_cache.CachedSession(".cache", expire_after=60)
            retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
            openmeteo = openmeteo_requests.Client(session=retry_session)
            responses = openmeteo.weather_api(url, params=params)

            all_data = [
                self.process_response(response, params, time_delta)
                for response in responses
            ]

            print(
                f"✅ OpenMeteo. "
                f" {start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')}. "
                f"Loaded {sum(df.shape[0] for df in all_data if df is not None)} records."
            )

            if merge:
                # Merge all dataframes into one
                all_data = pd.concat(all_data, ignore_index=True)

            print(
                f"✅ OpenMeteo. "
                f" {start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')}. "
                f"Loaded {all_data.shape[0]} records."
            )

            return all_data

        except Exception as e:
            print(f"❌ OpenMeteo. Error loading data: {str(e)}")
            return None

    def extract_data(self, time_series, variable_names):

        # create a dictionary to hold the data
        data = {
            "datetime": pd.date_range(
                start=pd.to_datetime(time_series.Time(), unit="s", utc=True),
                end=pd.to_datetime(time_series.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=time_series.Interval()),
                inclusive="left",
            )
        }

        # Loop through each variable and add it to the dictionary
        for i, var_name in enumerate(variable_names):
            data[var_name] = time_series.Variables(i).ValuesAsNumpy()

        # return a DataFrame from the dictionary
        return pd.DataFrame(data=data)

    def interpolate_hourly_data(self, hourly_data, minutely_15_datetimes):
        # Convert the datetime column to index for interpolation
        hourly_data.set_index("datetime", inplace=True)

        # Resample the hourly data to 15-minute intervals and interpolate
        interpolated = hourly_data.resample("15min").interpolate(method="linear")

        # Reindex to align with the minutely_15_datetimes
        interpolated = interpolated.reindex(minutely_15_datetimes, method="nearest")

        # Reset index to have datetime as a column again
        interpolated.reset_index(inplace=True)

        return interpolated
