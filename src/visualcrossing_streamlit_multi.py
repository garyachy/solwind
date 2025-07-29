import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, UTC
import matplotlib.pyplot as plt
import json
import os
from visualcrossing_api import VisualCrossingAPI
from visualcrossing_draw import VisualCrossingForecastDraw, VisualCrossingHistoryDraw
from config import get_config

# Load config
config = get_config()
visualcrossing_config = config.get("VisualCrossing", {})
location_config = config.get("Location", {})
API_KEY = visualcrossing_config.get("api_key")
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
page = st.sidebar.selectbox("Choose a page", ["Forecast Plots", "Historical Plots"])


# Initialize API and drawing classes
@st.cache_resource
def get_api():
    return VisualCrossingAPI(API_KEY)


@st.cache_resource
def get_forecast_draw():
    return VisualCrossingForecastDraw(get_api())


@st.cache_resource
def get_history_draw():
    return VisualCrossingHistoryDraw(get_api())


def get_available_parameters(lat, lon, start_date, end_date):
    """Get available parameters from a sample API call."""
    try:
        api = get_api()
        df = api.get_historical_data(lat, lon, start_date, end_date)
        if not df.empty:
            # Exclude metadata columns
            metadata_cols = {
                "timestamp",
                "latitude",
                "longitude",
                "datetime",
                "datetimeEpoch",
            }
            numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
            available_params = [col for col in numeric_cols if col not in metadata_cols]
            return available_params
    except Exception as e:
        st.warning(f"Could not fetch available parameters: {e}")
    return []


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

    # Parameter information
    st.subheader("Available Parameters")
    st.write(
        "The forecast will automatically plot all available numeric weather parameters including:"
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

            forecast_draw.plot_forecasts(
                locations=locations,
                start_datetime=start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                end_datetime=end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                label_locations=True,
            )
        except Exception as e:
            st.error(f"Error plotting forecast: {e}")

# Historical Plots Page
elif page == "Historical Plots":
    st.title("📊 Visual Crossing Historical Weather Data")
    st.write("Plot historical weather data for locations using Visual Crossing API.")

    # Location input
    st.subheader("Location Settings")
    col1, col2 = st.columns(2)
    with col1:
        hist_lat = st.number_input(
            "Latitude", value=float(DEFAULT_LAT), format="%.6f", key="hist_lat"
        )
    with col2:
        hist_lon = st.number_input(
            "Longitude", value=float(DEFAULT_LON), format="%.6f", key="hist_lon"
        )

    # Date range input
    st.subheader("Date Range Settings")
    today = datetime.now(UTC).date()
    yesterday = today - timedelta(days=1)

    col1, col2 = st.columns(2)
    with col1:
        hist_start_date = st.date_input(
            "Start date", value=yesterday, key="hist_start_date"
        )
    with col2:
        hist_end_date = st.date_input("End date", value=today, key="hist_end_date")

    # Multiple locations option
    st.subheader("Multiple Locations")
    use_multiple_hist_locations = st.checkbox(
        "Compare multiple locations", key="hist_multiple"
    )

    hist_locations = [(hist_lat, hist_lon)]
    if use_multiple_hist_locations:
        st.write("Add additional locations:")
        num_extra_hist_locations = st.number_input(
            "Number of additional locations",
            min_value=1,
            max_value=5,
            value=1,
            key="hist_extra",
        )

        for i in range(num_extra_hist_locations):
            col1, col2 = st.columns(2)
            with col1:
                extra_hist_lat = st.number_input(
                    f"Latitude {i+2}",
                    value=float(DEFAULT_LAT),
                    format="%.6f",
                    key=f"hist_extra_lat_{i}",
                )
            with col2:
                extra_hist_lon = st.number_input(
                    f"Longitude {i+2}",
                    value=float(DEFAULT_LON),
                    format="%.6f",
                    key=f"hist_extra_lon_{i}",
                )
            hist_locations.append((extra_hist_lat, extra_hist_lon))

    # Parameters selection
    st.subheader("Weather Parameters")
    st.write(
        "Select specific weather parameters to plot (leave empty for all available):"
    )

    # Get available parameters dynamically
    start_date_str = hist_start_date.strftime("%Y-%m-%d")
    end_date_str = hist_end_date.strftime("%Y-%m-%d")
    available_params = get_available_parameters(
        hist_lat, hist_lon, start_date_str, end_date_str
    )

    if available_params:
        st.success(f"Found {len(available_params)} available parameters")
        parameter_options = available_params
    else:
        st.warning("Could not detect available parameters. Using default list.")
        parameter_options = [
            "temp",
            "tempmax",
            "tempmin",
            "feelslike",
            "feelslikemax",
            "feelslikemin",
            "humidity",
            "dew",
            "precip",
            "precipprob",
            "precipcover",
            "preciptype",
            "snow",
            "snowdepth",
            "windgust",
            "windspeed",
            "winddir",
            "pressure",
            "visibility",
            "cloudcover",
            "solarradiation",
            "solarenergy",
            "uvindex",
            "severerisk",
            "moonphase",
            "sunriseEpoch",
            "sunsetEpoch",
        ]

    selected_parameters = st.multiselect(
        "Weather parameters",
        options=parameter_options,
        default=[],
        key="hist_parameters",
    )

    # Button to fetch and plot
    if st.button("Plot Historical Data", key="hist_plot_btn"):
        try:
            history_draw = get_history_draw()

            # Monkey patch plt.show for Streamlit
            def st_show(*args, **kwargs):
                st.pyplot(plt.gcf())
                plt.close()

            plt.show = st_show

            start_date_str = hist_start_date.strftime("%Y-%m-%d")
            end_date_str = hist_end_date.strftime("%Y-%m-%d")

            if len(hist_locations) == 1:
                st.info(
                    f"Plotting historical data for location: ({hist_lat:.4f}, {hist_lon:.4f})"
                )
                history_draw.plot_historical_data(
                    latitude=hist_lat,
                    longitude=hist_lon,
                    start_date=start_date_str,
                    end_date=end_date_str,
                    parameters=selected_parameters if selected_parameters else None,
                )
            else:
                st.info(
                    f"Plotting historical data comparison for {len(hist_locations)} locations"
                )
                history_draw.plot_historical_comparison(
                    locations=hist_locations,
                    start_date=start_date_str,
                    end_date=end_date_str,
                    parameters=selected_parameters if selected_parameters else None,
                )
        except Exception as e:
            st.error(f"Error plotting historical data: {e}")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**Powered by Visual Crossing Weather API**")
st.sidebar.markdown("Data provided by Visual Crossing")
