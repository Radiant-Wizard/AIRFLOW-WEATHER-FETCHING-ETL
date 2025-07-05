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
    df = df.drop_duplicates(subset=["extraction_date", "city"], keep="last", ignore_index=True)
    
    return df
def merge_data():
    # Relative path from $AIRFLOW_HOME
    
    base_dir = Path(os.getenv('AIRFLOW_HOME', Path(__file__).parent.parent))
    raw_dir = base_dir / "historical-data" / "raw"
    processed_dir = base_dir / "data" / "processed"
    output_file = processed_dir / "meteo_global.csv"
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    if not raw_dir.exists():
        raise FileNotFoundError(f"Input folder not found: {raw_dir}")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    global_df = pd.DataFrame()
    if output_file.exists() and output_file.stat().st_size > 0:
        try:
            global_df = pd.read_csv(output_file)
        except:
            print(f"Warning: Could not read existing output file {output_file}")
            global_df = pd.DataFrame()
    try:
        raw_dirs = [d for d in raw_dir.iterdir() if d.is_dir()]
    except:
        raise FileExistsError(f"There is no dir in {raw_dir}")
    
    for dir in raw_dirs:
        files = dir.glob("meteo_*.csv")
        
        if not files:
            print(f"No csv in {dir}")
            continue
        
        data = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
        
        global_df = pd.concat([global_df, data])
    
    if global_df.empty:
        raise ValueError("No data in all the csv")
    
    cleaned_df = _clean_df(global_df)
    
    cleaned_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    merge_data()