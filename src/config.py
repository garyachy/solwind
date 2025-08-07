import os
from dotenv import load_dotenv

load_dotenv()

def get_config():
    return {
        "Meteomatics": {
            "username": os.environ.get("METEOMATICS_USERNAME"),
            "password": os.environ.get("METEOMATICS_PASSWORD")
        },
        "Meteosource": {
            "api_key": os.environ.get("METEOSOURCE_API_KEY")
        },
        "VisualCrossing": {
            "api_key": os.environ.get("VISUALCROSSING_API_KEY")
        },
        "Openmeteo": {
            "api_key": os.environ.get("OPENMETEO_API_KEY")
        },
        "Meteoblue": {
            "api_key": os.environ.get("METEOBLUE_API_KEY")
        },
        "Windy": {
            "api_key": os.environ.get("WINDY_API_KEY")
        },
        "Stormglass": {
            "api_key": os.environ.get("STORMGLASS_API_KEY")
        },
        "Location": {
            "latitude": float(os.environ.get("LOCATION_LATITUDE", 0)),
            "longitude": float(os.environ.get("LOCATION_LONGITUDE", 0))
        }
    }