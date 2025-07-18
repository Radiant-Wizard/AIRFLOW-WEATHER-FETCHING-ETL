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
    numeric_cols = [
        'temperature','humidite','pluie_mm','meteo','temp_min','temp_max'
    ]
    
    numeric_cols = [
        col for col in df.columns 
        if any(pattern in col.lower() for pattern in numeric_cols)
        and df[col].dtype == object
    ]
    
    
    # Clean extraction_date
    if "extraction_date" in df.columns:
        # Convert to datetime (timezone-naive)
        df["extraction_date"] = pd.to_datetime(
            df["extraction_date"],
            errors='coerce',
            dayfirst=True,
            format='mixed'
        )
        
        # Handle remaining NaT values
        na_mask = df["extraction_date"].isna()
        if na_mask.any():
            df.loc[na_mask, "extraction_date"] = pd.to_datetime(
                df.loc[na_mask, "extraction_date"],
                errors='coerce',
                format='ISO8601'
            )
        
        # Normalize to date and ensure timezone-naive
        df["extraction_date"] = df["extraction_date"].dt.tz_localize(None).dt.normalize()
        
    # Make sheet not turning the float into date
    for col in numeric_cols:
        # Remove non-numeric characters (keep digits, decimal points, and negative signs)
        df[col] = df[col].astype(str).str.replace(r'[^\d.-]', '', regex=True)
        
        # Replace commas with dots for European decimal format
        df[col] = df[col].str.replace('.', ',', regex=False)
        
        # Convert to numeric, coercing errors to NaN
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Round to 2 decimal places if it's a float column
        if pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].round(2)
 
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
        print(dir.name)
        if not files:
            print(f"No csv in {dir}")
            continue
        
        data = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
        
        global_df = pd.concat([global_df, data])
    
    if global_df.empty:
        raise ValueError("No data in all the csv")
    
    global_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    merge_data()