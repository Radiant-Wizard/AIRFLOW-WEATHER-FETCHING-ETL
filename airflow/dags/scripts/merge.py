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
        df[col] = df[col].astype(str).str.replace(r'[^\d.-]', '', regex=True)
        
        df[col] = df[col].str.replace('.', ',', regex=False)
        
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].round(2)
 
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