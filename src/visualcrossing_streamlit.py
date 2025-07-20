import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import json
import os
from visualcrossing_api import VisualCrossingAPI, VisualCrossingDraw
from config import load_config

# Load config
config = load_config()
visualcrossing_config = config.get("VisualCrossing", {})
location_config = config.get("Location", {})
API_KEY = visualcrossing_config.get("api_key")
DEFAULT_LAT = location_config.get("latitude", 50.4501)
DEFAULT_LON = location_config.get("longitude", 30.5234)

# Streamlit UI
st.title("Visual Crossing 24h Weather Plotter")
st.write("Plot 24 hours of all available weather parameters for a location using Visual Crossing API.")

# Location input
lat = st.number_input("Latitude", value=float(DEFAULT_LAT), format="%.6f")
lon = st.number_input("Longitude", value=float(DEFAULT_LON), format="%.6f")

# Time range input (default: now aligned to previous 15-min, +24h)
now = datetime.utcnow().replace(second=0, microsecond=0)
minute = (now.minute // 15) * 15
aligned_now = now.replace(minute=minute)
default_end = aligned_now + timedelta(hours=24)

start_dt = st.datetime_input("Start datetime (UTC)", value=aligned_now)
end_dt = st.datetime_input("End datetime (UTC)", value=default_end)

# Button to fetch and plot
do_plot = st.button("Plot Forecast")

if do_plot:
    try:
        api = VisualCrossingAPI(API_KEY)
        draw = VisualCrossingDraw(api)
        # Instead of plt.show(), use st.pyplot
        # We'll monkeypatch plt.show to st.pyplot for this context
        def st_show(*args, **kwargs):
            st.pyplot(plt.gcf())
            plt.close()
        plt.show = st_show
        draw.plot_forecasts(
            locations=[(lat, lon)],
            start_datetime=start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            end_datetime=end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            label_locations=True,
        )
    except Exception as e:
        st.error(f"Error: {e}") 