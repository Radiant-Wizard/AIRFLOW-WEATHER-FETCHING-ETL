import pandas as pd
from pathlib import Path
import os

def _clean_df(df : pd.DataFrame ) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_") 
    )
    
    if "meteo" in df.columns:
        df["meteo"] = (
            df["meteo"]
            .astype(str)
            .str.lower()
            .str.strip()
            .str.replace(" ", "_")
        )
    
    if "extraction_date" in df.columns:
        df["extraction_date"] = pd.to_datetime(
            df["extraction_date"], errors="coerce", utc=True
        )

    # Make sheet not turning the float into date
    float_col = df.select_dtypes("float").columns
    df[float_col] = df[float_col].round(2)
    
    # Remove duplicates
    df = df.drop_duplicates(subset=["extraction_date", "city"], keep="last")
    
    return df
def merge_data(date: str) -> str:
    base_dir = Path(os.getenv('AIRFLOW_HOME', Path(__file__).parent.parent))
    input_dir = base_dir / "data" / "raw" / date
    output_file = base_dir / "data" / "processed" / "meteo_global.csv"

    if not input_dir.exists():
        raise FileNotFoundError(f"Input folder not found: {input_dir}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    if output_file.exists():
        global_df = pd.read_csv(
            output_file,
            parse_dates=["extraction_date"],
            date_format="ISO8601"
        )
    else:
        global_df = pd.DataFrame()

    new_data = []
    for file in input_dir.iterdir():
        if file.name.startswith("meteo_") and file.name.endswith(".csv"):
            df = pd.read_csv(
                file,
                parse_dates=["extraction_date"],
                date_format="ISO8601"
            )
            new_data.append(df)

    if not new_data:
        raise ValueError(f"No new data to merge for {date}")

    merged_df = pd.concat([global_df] + new_data, ignore_index=True)
    cleaned_df = _clean_df(merged_df)
    
    cleaned_df.to_csv(
        output_file, 
        index=False,
        date_format="%Y-%m-%d %H:%M:%S.%f"  # Preserve microsecond precision
    )
    return str(output_file)   