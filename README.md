# solwind
Multiple solar and wind meteo API 

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
