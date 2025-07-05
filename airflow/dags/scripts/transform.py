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
# ================= DIMENSION DATE =================
    dim_date_path = output_dir / "dim_date.csv"

    weather_data["extraction_date"] = pd.to_datetime(weather_data["extraction_date"])

    dates = weather_data["extraction_date"].dt.normalize().unique()

    if dim_date_path.exists():
        dim_date = pd.read_csv(dim_date_path, parse_dates=["date"])
    else:
        dim_date = pd.DataFrame(columns=[
            "date_id", "date", "year", "month", "day",
            "day_of_week", "is_weekend", "season"
        ])

    existing_dates = set(pd.to_datetime(dim_date["date"])) if not dim_date.empty else set()
    new_dates = set(dates) - existing_dates

    if new_dates:                                        # ← pas .empty
        to_append = pd.DataFrame({"date": pd.to_datetime(sorted(new_dates))})
        to_append = to_append.assign(
            year        = to_append["date"].dt.year,
            month       = to_append["date"].dt.month,
            day         = to_append["date"].dt.day,
            day_of_week = to_append["date"].dt.dayofweek,
            is_weekend  = to_append["date"].dt.dayofweek >= 5,
            season      = ((to_append["date"].dt.month % 12 + 3) // 3)
        ).reset_index(drop=True)

        start_id = int(dim_date["date_id"].max()) + 1 if not dim_date.empty else 1
        to_append.insert(0, "date_id", range(start_id, start_id + len(to_append)))

        dim_date = pd.concat([dim_date, to_append], ignore_index=True)
        dim_date.to_csv(dim_date_path, index=False)
    # ================= DIMENSION METEO =================
    dim_meteo_path = output_dir / "dim_meteo.csv"
    
    if dim_meteo_path.exists():
        dim_meteo = pd.read_csv(dim_meteo_path)
    else:
        # Mapping initial des codes météo (à adapter selon vos besoins)
        dim_meteo = pd.DataFrame({
            "meteo_id": [1, 2, 3, 4, 5],
            "code_meteo": [51, 80, 800, 801, 802],
            "description": ["bruine", "pluie légère", "ciel dégagé", "peu nuageux", "partiellement nuageux"],
            "severity": [1, 2, 0, 0, 1]
        })
        dim_meteo.to_csv(dim_meteo_path, index=False)
    
    # ================= TABLE DE FAITS =================
    fact_weather_path = output_dir / "fact_weather.csv"
    # Normalisation type
    weather_data["meteo"]      = weather_data["meteo"].astype(str)
    dim_meteo["code_meteo"]    = dim_meteo["code_meteo"].astype(str)
    
    # Jointure avec les dimensions
    fact_data = (
        weather_data
        .merge(city_dim[["city_id", "city"]], on="city", how="left")
        .merge(
            dim_date[["date_id", "date"]], 
            left_on=weather_data["extraction_date"].dt.normalize(), 
            right_on="date", 
            how="left"
        )
        .merge(dim_meteo[["meteo_id", "code_meteo"]], left_on="meteo", right_on="code_meteo", how="left")
        .drop(columns=["city", "extraction_date", "date", "code_meteo", "meteo"])
        .rename(columns={"meteo_id": "weather_condition_id"})
    )
    
    # Sauvegarde
    fact_data.to_csv(fact_weather_path, index=False)
    
    print(f"Star schema generated in {output_dir}")
    print(f"- Dimension City: {len(city_dim)} entries")
    print(f"- Dimension Date: {len(dim_date)} entries")
    print(f"- Dimension Meteo: {len(dim_meteo)} entries")
    print(f"- Fact Table: {len(fact_data)} weather records")
    
    return str(fact_weather_path)