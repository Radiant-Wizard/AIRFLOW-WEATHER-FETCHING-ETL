import pandas as pd
from pathlib import Path
import os

BASE_DIR = Path(os.getenv("AIRFLOW_HOME", Path(__file__).parent.parent))

def transform_to_star_schema() -> str:
    input_file   = BASE_DIR / "data" / "processed" / "meteo_global.csv"
    output_dir   = BASE_DIR / "data" / "star_schema"
    city_dim_path = output_dir / "dim_city.csv"
    fact_path     = output_dir / "fact_weather.csv"

    output_dir.mkdir(parents=True, exist_ok=True)
    weather_data = pd.read_csv(input_file)
    
    # -------- dimension: city_dim  -----------------------------------------
    
    if city_dim_path.exists():
        city_dim = pd.read_csv(city_dim_path)
    else:
        city_dim = pd.DataFrame(columns=["city_id", "city"])

    new_city = set(weather_data["city"]) - set(city_dim["city"])
    if new_city:
        next_id = (city_dim["city_id"].max() + 1) if not city_dim.empty else 1
        city_dim = pd.concat(
            [
                city_dim,
                pd.DataFrame(
                    {"city_id": range(next_id, next_id + len(new_city)),
                     "city": list(new_city)}
                ),
            ],
            ignore_index=True,
        )
        city_dim.to_csv(city_dim_path, index=False)

    # -------- fact: fact_weather -------------------------------------------
    fact_weather = (
        weather_data
        .merge(city_dim, on="city", how="left")
        .drop(columns=["city"])
    )

        
    fact_weather.to_csv(fact_path, index=False)

    return str(fact_path)   
