import os
import openmeteo_requests

from pathlib import Path
import pandas as pd
import requests_cache
from retry_requests import retry
CITIES  = ["Antananarivo", "London", "Paris", "Tokyo", "Sydney",
               "Reykjavik", "Moscow", "Cairo", "Cape Town", "Nairobi",
               "Mumbai", "Singapore", "Dubai", "New York",
               "Mexico City", "São Paulo", "Buenos Aires", "Auckland"]

BASE_DIR = Path(os.getenv("AIRFLOW_HOME", Path(__file__).parent.parent.parent))

def _base_get_past_data() -> pd.DataFrame :
	    # Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session =retry_session)  # type: ignore

	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below
	url = "https://archive-api.open-meteo.com/v1/archive"
	params = {
		"latitude": [-18.8792, 51.5072, 48.8566, 35.6895, -33.8688, 64.1466, 55.7558, 30.0444, -33.9249, -1.2921, 19.076, 1.3521, 25.2048, 40.7128, 19.4326, -23.5505, -34.6037, -36.8485],
		"longitude": [47.5079, -0.1276, 2.3522, 139.6917, 151.2093, -21.9426, 37.6173, 31.2357, 18.4241, 36.8219, 72.8777, 103.8198, 55.2708, -74.006, -99.1332, -46.6333, -58.3816, 174.7633],
		"start_date": ["2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05", "2024-07-05"],
		"end_date": ["2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30"],
		"daily": ["weather_code", "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "relative_humidity_2m_mean", "precipitation_sum"],
		"hourly": "temperature_2m",
		"timezone": ["Indian/Antananarivo", "Europe/London", "Europe/Paris", "Asia/Tokyo", "Australia/Sydney", "Atlantic/Reykjavik", "Europe/Moscow", "Africa/Cairo", "Africa/Johannesburg", "Africa/Nairobi", "Asia/Kolkata", "Asia/Singapore", "Asia/Dubai", "America/New_York", "America/Mexico_City", "America/Sao_Paulo", "America/Argentina/Buenos_Aires", "Pacific/Auckland"]
	}
	responses = openmeteo.weather_api(url, params=params)

	# Process first location. Add a for-loop for multiple locations or weather models
	response = responses[0]

	# Process hourly data. The order of variables needs to be the same as requested.
	hourly = response.Hourly()
	hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy() # type: ignore

	hourly_data = {"date": pd.date_range(
		start = pd.to_datetime(hourly.Time(), unit = "s", utc = True), # type: ignore
		end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),# type: ignore
		freq = pd.Timedelta(seconds = hourly.Interval()),# type: ignore
		inclusive = "left"
	)}

	hourly_data["temperature_2m"] = hourly_temperature_2m # type: ignore 

	hourly_dataframe = pd.DataFrame(data = hourly_data)

	# Process daily data. The order of variables needs to be the same as requested.
	daily = response.Daily()
	daily_weather_code = daily.Variables(0).ValuesAsNumpy()       # type: ignore
	daily_temperature_2m_mean = daily.Variables(1).ValuesAsNumpy()# type: ignore
	daily_temperature_2m_max = daily.Variables(2).ValuesAsNumpy()# type: ignore
	daily_temperature_2m_min = daily.Variables(3).ValuesAsNumpy()# type: ignore
	relative_humidity_2m_mean = daily.Variables(4).ValuesAsNumpy()# type: ignore
	precipitation_sum = daily.Variables(5).ValuesAsNumpy()# type: ignore

	daily_data = {"date": pd.date_range(
		start = pd.to_datetime(daily.Time(), unit = "s", utc = True),# type: ignore
		end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True), # type: ignore
		freq = pd.Timedelta(seconds = daily.Interval()),# type: ignore
		inclusive = "left"
	)}

	daily_data["weather_code"] = daily_weather_code        			# type: ignore
	daily_data["temperature_2m_mean"] = daily_temperature_2m_mean# type: ignore
	daily_data["temperature_2m_max"] = daily_temperature_2m_max# type: ignore
	daily_data["temperature_2m_min"] = daily_temperature_2m_min# type: ignore
	daily_data["relative_humidity_2m_mean"] = relative_humidity_2m_mean# type: ignore
	daily_data["precipitation_sum"] = precipitation_sum# type: ignore

	daily_dataframe = pd.DataFrame(data = daily_data)
	print(daily_dataframe.info())
	return daily_dataframe
	
def main() -> bool:
	data = _base_get_past_data()
	records: list[dict] = []
	for city in CITIES:
		for i, date in enumerate(data["date"]):
			records.append({
				"city": city,
    			"extraction_date": date,
				"temperature": data["temperature_2m_mean"][i],
            	"humidite": data.get("relative_humidity_2m_mean",[None] * len(data["date"]))[i],
            	"pluie_mm":	data["precipitation_sum"][i],
            	"meteo":	data["weather_code"][i],
            	"temp_min":	data["temperature_2m_min"][i],
        		"temp_max":	data["temperature_2m_max"][i],
			})

	for record in records:
		df = pd.DataFrame([record])
		simple_date = record["extraction_date"].strftime("%Y-%m-%d") 
		target_dir = BASE_DIR / "historical-data" / "raw" / simple_date
		target_dir.mkdir(parents=True, exist_ok=True)

		out_file = target_dir / f"meteo_{str(record['city'])}.csv"
		df.to_csv(out_file, index=False, float_format="%.2f")


	print("Historical data has been backed-up successfully...")
	return True
	
if __name__ == "__main__":
    main()
    
    
