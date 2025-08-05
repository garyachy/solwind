import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, UTC
import matplotlib.pyplot as plt
import json
import os
from visualcrossing_api import VisualCrossingAPI
from visualcrossing_draw import VisualCrossingForecastDraw
from meteomatics_api import MeteomaticsAPI
from meteomatics_draw import MeteomaticsForecastDraw
from combined_draw import CombinedForecastDraw
from config import get_config

# Load config
config = get_config()
visualcrossing_config = config.get("VisualCrossing", {})
meteomatics_config = config.get("Meteomatics", {})
location_config = config.get("Location", {})
API_KEY = visualcrossing_config.get("api_key")
METEOMATICS_USERNAME = meteomatics_config.get("username")
METEOMATICS_PASSWORD = meteomatics_config.get("password")
DEFAULT_LAT = location_config.get("latitude", 50.4501)
DEFAULT_LON = location_config.get("longitude", 30.5234)

# Page configuration
st.set_page_config(
    page_title="Visual Crossing Weather Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Choose a page", ["Forecast Plots", "Meteomatics Plots", "API Comparison"])


# Initialize API and drawing classes
@st.cache_resource
def get_api():
    return VisualCrossingAPI(API_KEY)


@st.cache_resource
def get_forecast_draw():
    return VisualCrossingForecastDraw(get_api())


@st.cache_resource
def get_meteomatics_api():
    if METEOMATICS_USERNAME and METEOMATICS_PASSWORD:
        return MeteomaticsAPI(METEOMATICS_USERNAME, METEOMATICS_PASSWORD)
    return None


@st.cache_resource
def get_meteomatics_draw():
    api = get_meteomatics_api()
    if api:
        return MeteomaticsForecastDraw(api)
    return None


@st.cache_resource
def get_combined_draw():
    vc_api = get_api()
    mm_api = get_meteomatics_api()
    if vc_api and mm_api:
        return CombinedForecastDraw(vc_api, mm_api)
    return None





# Forecast Plots Page
if page == "Forecast Plots":
    st.title("🌤️ Visual Crossing Weather Forecast")
    st.write("Plot weather forecasts for locations using Visual Crossing API.")

    # Location input
    st.subheader("Location Settings")
    col1, col2 = st.columns(2)
    with col1:
        lat = st.number_input(
            "Latitude", value=float(DEFAULT_LAT), format="%.6f", key="forecast_lat"
        )
    with col2:
        lon = st.number_input(
            "Longitude", value=float(DEFAULT_LON), format="%.6f", key="forecast_lon"
        )

    # Time range input
    st.subheader("Time Range Settings")
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    minute = (now.minute // 15) * 15
    aligned_now = now.replace(minute=minute)
    default_end = aligned_now + timedelta(hours=24)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start date (UTC)", value=aligned_now.date(), key="forecast_start_date"
        )
        start_time = st.time_input(
            "Start time (UTC)", value=aligned_now.time(), key="forecast_start_time"
        )
    with col2:
        end_date = st.date_input(
            "End date (UTC)", value=default_end.date(), key="forecast_end_date"
        )
        end_time = st.time_input(
            "End time (UTC)", value=default_end.time(), key="forecast_end_time"
        )

    start_dt = datetime.combine(start_date, start_time)
    end_dt = datetime.combine(end_date, end_time)

    # Multiple locations option
    st.subheader("Multiple Locations")
    use_multiple_locations = st.checkbox(
        "Compare multiple locations", key="forecast_multiple"
    )

    locations = [(lat, lon)]
    if use_multiple_locations:
        st.write("Add additional locations:")
        num_extra_locations = st.number_input(
            "Number of additional locations",
            min_value=1,
            max_value=5,
            value=1,
            key="forecast_extra",
        )

        for i in range(num_extra_locations):
            col1, col2 = st.columns(2)
            with col1:
                extra_lat = st.number_input(
                    f"Latitude {i+2}",
                    value=float(DEFAULT_LAT),
                    format="%.6f",
                    key=f"forecast_extra_lat_{i}",
                )
            with col2:
                extra_lon = st.number_input(
                    f"Longitude {i+2}",
                    value=float(DEFAULT_LON),
                    format="%.6f",
                    key=f"forecast_extra_lon_{i}",
                )
            locations.append((extra_lat, extra_lon))

    # Parameter selection
    st.subheader("Weather Parameters")
    st.write("Select specific weather parameters to plot (leave empty for all available):")
    
    # VisualCrossing parameters
    visualcrossing_parameters = [
        "temp",  # Temperature
        "feelslike",  # Feels like temperature
        "humidity",  # Humidity
        "dew",  # Dew point
        "precip",  # Precipitation
        "precipprob",  # Precipitation probability
        "precipcover",  # Precipitation coverage
        "preciptype",  # Precipitation type
        "snow",  # Snow
        "snowdepth",  # Snow depth
        "windspeed",  # Wind speed
        "winddir",  # Wind direction
        "pressure",  # Pressure
        "cloudcover",  # Cloud cover
        "visibility",  # Visibility
        "solarradiation",  # Solar radiation
        "solarenergy",  # Solar energy
        "uvindex",  # UV index
        "severerisk",  # Severe risk
        "conditions",  # Weather conditions
        "icon",  # Weather icon
        "stations",  # Weather stations
        "source",  # Data source
    ]
    
    selected_parameters = st.multiselect(
        "Select parameters",
        options=visualcrossing_parameters,
        default=[],
        help="Leave empty to plot all available parameters",
        key="forecast_parameters"
    )

    # Parameter information
    st.subheader("Available Parameters")
    st.write(
        "If no specific parameters are selected, the forecast will automatically plot all available numeric weather parameters including:"
    )
    st.write(
        "Temperature, humidity, wind, precipitation, pressure, solar radiation, and more."
    )

    # Button to fetch and plot
    if st.button("Plot Forecast", key="forecast_plot_btn"):
        try:
            forecast_draw = get_forecast_draw()

            # Monkey patch plt.show for Streamlit
            def st_show(*args, **kwargs):
                st.pyplot(plt.gcf())
                plt.close()

            plt.show = st_show

            if len(locations) == 1:
                st.info(f"Plotting forecast for location: ({lat:.4f}, {lon:.4f})")
            else:
                st.info(f"Plotting forecast for {len(locations)} locations")

            # Use selected parameters if any, otherwise None for all available
            parameters = selected_parameters if selected_parameters else None
            
            forecast_draw.plot_forecasts(
                locations=locations,
                start_datetime=start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                end_datetime=end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                parameters=parameters,
                label_locations=True,
            )
        except Exception as e:
            st.error(f"Error plotting forecast: {e}")


# API Comparison Page
elif page == "API Comparison":
    st.title("🌤️ Weather API Comparison")
    st.write("Compare weather forecasts from Visual Crossing and Meteomatics APIs.")

    # Check if both APIs are available
    meteomatics_available = METEOMATICS_USERNAME and METEOMATICS_PASSWORD
    visualcrossing_available = API_KEY

    if not meteomatics_available:
        st.warning("Meteomatics credentials not configured. Only Visual Crossing data will be shown.")
    
    if not visualcrossing_available:
        st.warning("Visual Crossing API key not configured. Only Meteomatics data will be shown.")

    if not meteomatics_available and not visualcrossing_available:
        st.error("No weather APIs are configured. Please check your configuration.")
        st.stop()

    # Location input
    st.subheader("Location Settings")
    col1, col2 = st.columns(2)
    with col1:
        lat = st.number_input(
            "Latitude", value=float(DEFAULT_LAT), format="%.6f", key="comparison_lat"
        )
    with col2:
        lon = st.number_input(
            "Longitude", value=float(DEFAULT_LON), format="%.6f", key="comparison_lon"
        )

    # Time range input
    st.subheader("Time Range Settings")
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    minute = (now.minute // 15) * 15
    aligned_now = now.replace(minute=minute)
    default_end = aligned_now + timedelta(hours=24)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start date (UTC)", value=aligned_now.date(), key="comparison_start_date"
        )
        start_time = st.time_input(
            "Start time (UTC)", value=aligned_now.time(), key="comparison_start_time"
        )
    with col2:
        end_date = st.date_input(
            "End date (UTC)", value=default_end.date(), key="comparison_end_date"
        )
        end_time = st.time_input(
            "End time (UTC)", value=default_end.time(), key="comparison_end_time"
        )

    start_dt = datetime.combine(start_date, start_time)
    end_dt = datetime.combine(end_date, end_time)

    # Parameters to compare
    st.subheader("Parameters to Compare")
    st.write("Select weather parameters to compare between APIs:")
    
    # Common parameters that both APIs might have
    common_parameters = [
        "temp", "temperature", "t_2m:C",  # Temperature
        "humidity", "rh_2m:p",  # Humidity
        "windspeed", "wind_speed_10m:ms",  # Wind speed
        "winddir", "wind_dir_10m:d",  # Wind direction
        "pressure", "msl_pressure:hPa",  # Pressure
        "precip", "precip_1h:mm",  # Precipitation
    ]
    
    # VisualCrossing specific parameters
    visualcrossing_parameters = [
        "temp",  # Temperature
        "feelslike",  # Feels like temperature
        "humidity",  # Humidity
        "dew",  # Dew point
        "precip",  # Precipitation
        "precipprob",  # Precipitation probability
        "precipcover",  # Precipitation coverage
        "preciptype",  # Precipitation type
        "snow",  # Snow
        "snowdepth",  # Snow depth
        "windspeed",  # Wind speed
        "winddir",  # Wind direction
        "pressure",  # Pressure
        "cloudcover",  # Cloud cover
        "visibility",  # Visibility
        "solarradiation",  # Solar radiation
        "solarenergy",  # Solar energy
        "uvindex",  # UV index
        "severerisk",  # Severe risk
        "conditions",  # Weather conditions
        "icon",  # Weather icon
        "stations",  # Weather stations
        "source",  # Data source
    ]
    
    # Meteomatics specific parameters
    meteomatics_parameters = [
        "t_2m:C",  # Temperature at 2m
        "rh_2m:p",  # Relative humidity at 2m
        "wind_speed_10m:ms",  # Wind speed at 10m
        "wind_dir_10m:d",  # Wind direction at 10m
        "msl_pressure:hPa",  # Mean sea level pressure
        "precip_1h:mm",  # 1-hour precipitation
        "precip_total:mm",  # Total precipitation
        "solar_radiation:W",  # Solar radiation
        "cloud_cover:p",  # Cloud cover percentage
        "visibility:m",  # Visibility
        "dew_point_2m:C",  # Dew point at 2m
        "feels_like_2m:C",  # Feels like temperature
    ]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("VisualCrossing Parameters")
        selected_visualcrossing_parameters = st.multiselect(
            "Select VisualCrossing parameters",
            options=visualcrossing_parameters,
            default=["temp"],
            key="comparison_visualcrossing_parameters",
            help="Choose which VisualCrossing parameters to include in the comparison"
        )
    
    with col2:
        st.subheader("Meteomatics Parameters")
        selected_meteomatics_parameters = st.multiselect(
            "Select Meteomatics parameters",
            options=meteomatics_parameters,
            default=["t_2m:C"],
            key="comparison_meteomatics_parameters",
            help="Choose which Meteomatics parameters to include in the comparison"
        )

    # API settings
    st.subheader("API Settings")
    col1, col2 = st.columns(2)
    with col1:
        unit_group = st.selectbox(
            "Visual Crossing Unit Group",
            options=["metric", "us", "uk"],
            index=0,
            key="comparison_unit_group"
        )
    with col2:
        model = st.selectbox(
            "Meteomatics Model",
            options=["mix", "gfs", "ecmwf", "icon"],
            index=0,
            key="comparison_model"
        )

    # Button to fetch and plot comparison
    if st.button("Compare APIs", key="comparison_plot_btn"):
        try:
            combined_draw = get_combined_draw()
            
            if combined_draw:
                # Monkey patch plt.show for Streamlit
                def st_show(*args, **kwargs):
                    st.pyplot(plt.gcf())
                    plt.close()

                plt.show = st_show

                st.info(f"Comparing APIs for location: ({lat:.4f}, {lon:.4f})")
                st.info(f"VisualCrossing parameters: {', '.join(selected_visualcrossing_parameters)}")
                st.info(f"Meteomatics parameters: {', '.join(selected_meteomatics_parameters)}")

                combined_draw.plot_comparison(
                    locations=[(lat, lon)],
                    parameters=selected_meteomatics_parameters,
                    start_datetime=start_dt,
                    end_datetime=end_dt,
                    unit_group=unit_group,
                    model=model,
                    visualcrossing_parameters=selected_visualcrossing_parameters,
                )
            else:
                st.error("Unable to initialize both APIs. Please check your configuration.")
                
        except Exception as e:
            st.error(f"Error comparing APIs: {e}")


# Meteomatics Plots Page
elif page == "Meteomatics Plots":
    st.title("🌤️ Meteomatics Weather Forecast")
    st.write("Plot weather forecasts for locations using Meteomatics API.")

    # Check if Meteomatics API is available
    meteomatics_available = METEOMATICS_USERNAME and METEOMATICS_PASSWORD
    if not meteomatics_available:
        st.error("Meteomatics credentials not configured. Please check your configuration.")
        st.stop()

    # Location input
    st.subheader("Location Settings")
    col1, col2 = st.columns(2)
    with col1:
        lat = st.number_input(
            "Latitude", value=float(DEFAULT_LAT), format="%.6f", key="meteomatics_lat"
        )
    with col2:
        lon = st.number_input(
            "Longitude", value=float(DEFAULT_LON), format="%.6f", key="meteomatics_lon"
        )

    # Time range input
    st.subheader("Time Range Settings")
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    minute = (now.minute // 15) * 15
    aligned_now = now.replace(minute=minute)
    default_end = aligned_now + timedelta(hours=24)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start date (UTC)", value=aligned_now.date(), key="meteomatics_start_date"
        )
        start_time = st.time_input(
            "Start time (UTC)", value=aligned_now.time(), key="meteomatics_start_time"
        )
    with col2:
        end_date = st.date_input(
            "End date (UTC)", value=default_end.date(), key="meteomatics_end_date"
        )
        end_time = st.time_input(
            "End time (UTC)", value=default_end.time(), key="meteomatics_end_time"
        )

    start_dt = datetime.combine(start_date, start_time)
    end_dt = datetime.combine(end_date, end_time)

    # Multiple locations option
    st.subheader("Multiple Locations")
    use_multiple_locations = st.checkbox(
        "Compare multiple locations", key="meteomatics_multiple"
    )

    locations = [(lat, lon)]
    if use_multiple_locations:
        st.write("Add additional locations:")
        num_extra_locations = st.number_input(
            "Number of additional locations",
            min_value=1,
            max_value=5,
            value=1,
            key="meteomatics_extra",
        )

        for i in range(num_extra_locations):
            col1, col2 = st.columns(2)
            with col1:
                extra_lat = st.number_input(
                    f"Latitude {i+2}",
                    value=float(DEFAULT_LAT),
                    format="%.6f",
                    key=f"meteomatics_extra_lat_{i}",
                )
            with col2:
                extra_lon = st.number_input(
                    f"Longitude {i+2}",
                    value=float(DEFAULT_LON),
                    format="%.6f",
                    key=f"meteomatics_extra_lon_{i}",
                )
            locations.append((extra_lat, extra_lon))

    # Meteomatics specific settings
    st.subheader("Meteomatics Settings")
    col1, col2 = st.columns(2)
    with col1:
        model = st.selectbox(
            "Weather Model",
            options=["mix", "gfs", "ecmwf", "icon"],
            index=0,
            help="Weather model to use for forecasting",
            key="meteomatics_model"
        )
    with col2:
        interval_hours = st.selectbox(
            "Data Interval",
            options=[1, 3, 6, 12, 24],
            index=0,
            help="Time interval between data points in hours",
            key="meteomatics_interval"
        )

    # Parameter selection
    st.subheader("Weather Parameters")
    st.write("Select specific weather parameters to plot (leave empty for all available):")
    
    # Common Meteomatics parameters
    meteomatics_parameters = [
        "t_2m:C",  # Temperature at 2m
        "rh_2m:p",  # Relative humidity at 2m
        "wind_speed_10m:ms",  # Wind speed at 10m
        "wind_dir_10m:d",  # Wind direction at 10m
        "msl_pressure:hPa",  # Mean sea level pressure
        "precip_1h:mm",  # 1-hour precipitation
        "precip_total:mm",  # Total precipitation
        "solar_radiation:W",  # Solar radiation
        "cloud_cover:p",  # Cloud cover percentage
        "visibility:m",  # Visibility
        "dew_point_2m:C",  # Dew point at 2m
        "feels_like_2m:C",  # Feels like temperature
    ]
    
    selected_parameters = st.multiselect(
        "Select parameters",
        options=meteomatics_parameters,
        default=[],
        help="Leave empty to plot all available parameters",
        key="meteomatics_parameters"
    )

    # Parameter information
    st.subheader("Available Parameters")
    st.write(
        "If no specific parameters are selected, the forecast will automatically plot all available numeric weather parameters including:"
    )
    st.write(
        "Temperature, humidity, wind, precipitation, pressure, solar radiation, cloud cover, visibility, and more."
    )

    # Button to fetch and plot
    if st.button("Plot Meteomatics Forecast", key="meteomatics_plot_btn"):
        try:
            meteomatics_draw = get_meteomatics_draw()
            
            if meteomatics_draw:
                # Monkey patch plt.show for Streamlit
                def st_show(*args, **kwargs):
                    st.pyplot(plt.gcf())
                    plt.close()

                plt.show = st_show

                if len(locations) == 1:
                    st.info(f"Plotting Meteomatics forecast for location: ({lat:.4f}, {lon:.4f})")
                else:
                    st.info(f"Plotting Meteomatics forecast for {len(locations)} locations")

                # Use selected parameters if any, otherwise None for all available
                parameters = selected_parameters if selected_parameters else None
                
                # Set interval as timedelta
                interval = timedelta(hours=interval_hours)

                if len(locations) == 1:
                    # Single location - use plot_forecasts
                    meteomatics_draw.plot_forecasts(
                        locations=locations,
                        parameters=parameters,
                        start_datetime=start_dt,
                        end_datetime=end_dt,
                        model=model,
                        interval=interval,
                        label_locations=True,
                    )
                else:
                    # Multiple locations - use plot_comparison
                    meteomatics_draw.plot_comparison(
                        locations=locations,
                        parameters=parameters or ["t_2m:C"],  # Default to temperature if no parameters selected
                        start_datetime=start_dt,
                        end_datetime=end_dt,
                        model=model,
                        interval=interval,
                    )
            else:
                st.error("Unable to initialize Meteomatics API. Please check your configuration.")
                
        except Exception as e:
            st.error(f"Error plotting Meteomatics forecast: {e}")


# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**Powered by Visual Crossing Weather API & Meteomatics**")
st.sidebar.markdown("Data provided by Visual Crossing and Meteomatics")
