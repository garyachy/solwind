# solwind
Multiple solar and wind meteo API 

## Features

- **High-Resolution Weather Data**: Meteomatics API now supports 15-minute resolution for detailed weather forecasts
- **Multiple Weather APIs**: Support for Meteomatics, Visual Crossing, Meteosource, OpenMeteo, Meteoblue, and Windy
- **Interactive Dashboards**: Streamlit-based visualization tools
- **Comprehensive Testing**: Extensive test suite for all API integrations

## Configuration

1. Copy `config.json` to `config.example.json` and fill with your own secrets or use environment variables.
2. Create a `.env` file in the project root with your secrets:

```
METEOMATICS_USERNAME=your_username
METEOMATICS_PASSWORD=your_password
METEOSOURCE_API_KEY=your_api_key
VISUALCROSSING_API_KEY=your_api_key
OPENMETEO_API_KEY=your_api_key
METEOBLUE_API_KEY=your_api_key
WINDY_API_KEY=your_api_key
LOCATION_LATITUDE=50.4501
LOCATION_LONGITUDE=30.5234
```

3. Never commit `config.json` or `.env` to git.

## Meteomatics API - 15-Minute Resolution

The Meteomatics API has been enhanced to support high-resolution weather data with 15-minute intervals. This provides much more detailed weather forecasts compared to the standard 1-hour intervals.

### Usage Examples

```python
from meteomatics_api import MeteomaticsAPI
import datetime as dt

# Initialize API
api = MeteomaticsAPI(username, password)

# Get high-resolution forecast (15-minute intervals)
high_res_results = api.get_high_resolution_forecast(
    locations=[(50.4501, 30.5234)],
    parameters=["t_2m:C", "precip_1h:mm"],
    start_datetime=dt.datetime.now(dt.timezone.utc),
    end_datetime=dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=6)
)

# Get standard resolution forecast (1-hour intervals)
standard_results = api.get_standard_forecast(
    locations=[(50.4501, 30.5234)],
    parameters=["t_2m:C"],
    start_datetime=dt.datetime.now(dt.timezone.utc),
    end_datetime=dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=24)
)

# Custom interval (default is now 15 minutes)
custom_results = api.get_forecast(
    locations=[(50.4501, 30.5234)],
    parameters=["t_2m:C"],
    start_datetime=dt.datetime.now(dt.timezone.utc),
    end_datetime=dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=2),
    interval=dt.timedelta(minutes=30)  # 30-minute intervals
)
```

### Supported Intervals

The Meteomatics API supports the following intervals:
- 15 minutes (default for high-resolution data)
- 30 minutes
- 1 hour
- 3 hours
- 6 hours
- 12 hours
- 1 day

### Benefits of 15-Minute Resolution

- **More Accurate Forecasts**: Higher temporal resolution provides more precise weather predictions
- **Better for Solar/Wind Applications**: Critical for renewable energy forecasting
- **Detailed Analysis**: Enables fine-grained weather pattern analysis
- **Real-time Monitoring**: Better suited for real-time weather monitoring applications

## Running the Application

### Example Usage
```bash
python example_combined_usage.py
```

### Interactive Dashboard
```bash
streamlit run src/visualcrossing_streamlit_multi.py
```

**Features:**
- **Meteomatics Plots**: Always uses 15-minute resolution for high-quality data
- **API Comparison**: Meteomatics data uses 15-minute intervals, Visual Crossing uses hourly intervals
- **High-Resolution Data**: 4x more data points for detailed weather analysis

### Running Tests
```bash
# Run all tests
pytest tests/

# Run specific test suites
pytest tests/test_meteomatics.py
pytest tests/test_meteomatics_15min.py
```

## API Documentation

### Meteomatics API Methods

- `get_forecast()`: Main method with customizable interval (defaults to 15 minutes)
- `get_high_resolution_forecast()`: Dedicated method for 15-minute resolution
- `get_standard_forecast()`: Dedicated method for 1-hour resolution

### Data Format

All API methods return a list of pandas DataFrames, one for each location, with the following columns:
- `datetime`: UTC timestamp
- `latitude`, `longitude`: Location coordinates
- Weather parameters (e.g., `t_2m:C` for temperature)

## Testing

The project includes comprehensive tests for:
- 15-minute resolution functionality
- Standard resolution functionality
- Interval validation
- Data format consistency
- API error handling

Run the tests to verify your setup:
```bash
pytest tests/test_meteomatics_15min.py -v
```
