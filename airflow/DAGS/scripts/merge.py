import pandas as pd
from pathlib import Path
import os


def merge_data(date: str) -> str:
    # Relative path from $AIRFLOW_HOME
    base_dir = Path(os.getenv('AIRFLOW_HOME', Path(__file__).parent.parent))
    input_dir = base_dir / "data" / "raw" / date
    output_file = base_dir / "data" / "processed" / "meteo_global.csv"

    # Checking if input_dir exists
    if not input_dir.exists():
        raise FileNotFoundError(f"Input folder not found: {input_dir}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Read or initialise input dir
    global_df = pd.read_csv(output_file) if output_file.exists() else pd.DataFrame()

    # Concatenate raw data 
    new_data = [
        pd.read_csv(file)
        for file in input_dir.iterdir()
        if file.name.startswith("meteo_") and file.name.endswith(".csv")
    ]

    if not new_data:
        raise ValueError(f"No new data to fuse for {date}")

    updated_df = pd.concat([global_df] + new_data, ignore_index=True)
    updated_df = updated_df.drop_duplicates(
        subset=["ville", "date_extraction"],
        keep="last"
    )

    updated_df.to_csv(output_file, index=False)
    return str(output_file)
