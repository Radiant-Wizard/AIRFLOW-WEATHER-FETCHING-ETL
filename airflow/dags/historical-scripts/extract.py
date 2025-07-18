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

def _base_get_past_data() -> dict[str, pd.DataFrame]:
	    # Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session =retry_session)  # type: ignore

	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below
	url = "https://archive-api.open-meteo.com/v1/archive"
	params = {
		"latitude": [-18.8792, 51.5074, 48.8566, 35.6895, -33.8688, 64.1466, 55.7558, 30.0444, -33.9249, -1.2921, 19.076, 1.3521, 25.2048, 40.7128, 19.4326, -23.5505, -34.6037, -36.8485],
		"longitude": [47.5079, -0.1278, 2.3522, 139.6917, 151.2093, -21.9426, 37.6173, 31.2357, 18.4241, 36.8219, 72.8777, 103.8198, 55.2708, -74.006, -99.1332, -46.6333, -58.3816, 174.7633],
		"start_date": ["2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01"],
		"end_date": ["2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16", "2025-07-16"],
		"daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "weather_code", "relative_humidity_2m_mean", "temperature_2m_mean"],
		"timezone": ["Indian/Antananarivo", "Europe/London", "Europe/Paris", "Asia/Tokyo", "Australia/Sydney", "Atlantic/Reykjavik", "Europe/Moscow", "Africa/Cairo", "Africa/Johannesburg", "Africa/Nairobi", "Asia/Kolkata", "Asia/Singapore", "Asia/Dubai", "America/New_York", "America/Mexico_City", "America/Sao_Paulo", "America/Argentina/Buenos_Aires", "Pacific/Auckland"]
	}
	responses = openmeteo.weather_api(url, params=params)

	city_dataframes: dict[str, pd.DataFrame] = {}

	for city, response in zip(CITIES, responses):
		daily = response.Daily()

		data = {
			"date": pd.date_range(
				start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
				end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
				freq = pd.Timedelta(seconds = daily.Interval()),
				inclusive = "left"
			),
			"temperature_2m_max": daily.Variables(0).ValuesAsNumpy(),
			"temperature_2m_min": daily.Variables(1).ValuesAsNumpy(),
			"precipitation_sum": daily.Variables(2).ValuesAsNumpy(),
			"weather_code": daily.Variables(3).ValuesAsNumpy(),
			"relative_humidity_2m_mean": daily.Variables(4).ValuesAsNumpy(),
			"temperature_2m_mean": daily.Variables(5).ValuesAsNumpy(),
		}
		city_dataframes[city] = pd.DataFrame(data)

	return city_dataframes

	# # Process first location. Add a for-loop for multiple locations or weather models
	# response = responses[0]
	# print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
	# print(f"Elevation {response.Elevation()} m asl")
	# print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
	# print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

	# # Process daily data. The order of variables needs to be the same as requested.
	# daily = response.Daily()
	# daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
	# daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
	# daily_precipitation_sum = daily.Variables(2).ValuesAsNumpy()
	# daily_weather_code = daily.Variables(3).ValuesAsNumpy()
	# daily_relative_humidity_2m_mean = daily.Variables(4).ValuesAsNumpy()
	# daily_temperature_2m_mean = daily.Variables(5).ValuesAsNumpy()

	# daily_data = {"date": pd.date_range(
	# 	start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
	# 	end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
	# 	freq = pd.Timedelta(seconds = daily.Interval()),
	# 	inclusive = "left"
	# )}

	# daily_data["temperature_2m_max"] = daily_temperature_2m_max
	# daily_data["temperature_2m_min"] = daily_temperature_2m_min
	# daily_data["precipitation_sum"] = daily_precipitation_sum
	# daily_data["weather_code"] = daily_weather_code
	# daily_data["relative_humidity_2m_mean"] = daily_relative_humidity_2m_mean
	# daily_data["temperature_2m_mean"] = daily_temperature_2m_mean

	# daily_dataframe = pd.DataFrame(data = daily_data)
	# return daily_dataframe
	
def main() -> bool:
	all_city_data = _base_get_past_data()
	records: list[dict] = []

	for city, data in all_city_data.items():
		for i, date in enumerate(data["date"]):
			records.append({
				"city": city,
    			"extraction_date": date,
				"temperature": data["temperature_2m_mean"][i],
            	"humidite": data["relative_humidity_2m_mean"][i],
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
    
    
