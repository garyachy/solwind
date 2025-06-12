from config import load_config
import pytest
import pandas as pd
from datetime import datetime, timedelta
from openmeteo import *


def test_openmeteo_load_data():
    config = load_config()

    openmeteo_config = config.get("Openmeteo", {})
    location_config = config.get("Location", {})

    latitude = location_config.get("latitude")
    longitude = location_config.get("longitude")
    api_key = openmeteo_config.get("api_key")

    openmeteo_instance = OpenMeteo(api_key)