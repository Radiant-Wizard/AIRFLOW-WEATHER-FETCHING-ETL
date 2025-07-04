from datetime import datetime                    # ← you need datetime.datetime, not datetime module
from pathlib import Path
import os
import requests
import pandas as pd

BASE_DIR = Path(os.getenv("AIRFLOW_HOME", Path(__file__).parent.parent))

def extract_forecast_data(city: str, api_key: str, date: str) -> bool:
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": api_key, "units": "metric", "lang": "fr"}

    resp = requests.get(url, params=params, timeout=100)
    resp.raise_for_status()                                # raises if HTTP error

    data = resp.json()
    record = {
        "city":          city,
        "extraction_date": datetime.now().strftime("%y-%m-%d"),
        "temperature":    data["main"]["temp"],
        "humidite":       data["main"]["humidity"],
        "pluie_mm": data.get("rain", {}).get("1h", 0),
        "meteo": data["weather"][0]["main"],
        "temp_min":     data["main"]["temp_min"],
        "temp_max":     data["main"]["temp_max"],
    }

    # data path
    target_dir = BASE_DIR / "data" / "raw" / date
    target_dir.mkdir(parents=True, exist_ok=True)

    out_file = target_dir / f"meteo_{city}.csv"
    # ------ cleaning --------
    df = pd.DataFrame([record])
    df.to_csv(out_file, index=False, float_format="%.2f")
    return True