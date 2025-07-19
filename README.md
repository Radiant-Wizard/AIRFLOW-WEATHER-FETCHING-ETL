# 🌦️ AIRFLOW-WEATHER-FETCHING-ETL

A complete **Apache Airflow ETL pipeline** for fetching, transforming, and loading weather data from **OpenWeatherMap** and **Open-Meteo** APIs. The pipeline supports daily updates and optional syncing to **Google Drive**.

---

## Project Information

- **Python Version**: `3.10.12`
- **Airflow Version**: `3.0.2`

To ensure proper configuration, run the following command in the root of the project:

```bash
echo 'export AIRFLOW_HOME="$(pwd)/airflow"' >> .venv/bin/activate
```

> This project **must be run inside a virtual environment**. Dependencies are listed in [`requirements.txt`](./requirements.txt).

---

## Project Structure

```
AIRFLOW-WEATHER-FETCHING-ETL/
│
├── airflow/                            # Main Airflow directory
│   ├── dags/
│   │   ├── weather_etl.py              # Main DAG
│   │   ├── scripts/                    # Daily ETL scripts
│   │   └── historical-scripts/        # One-time historical ETL
│   ├── airflow.cfg                     # Airflow configuration
│   └── webserver_config.py            # Webserver settings (optional)
│
├── data/
│   ├── raw/
│   │   ├── 2025-07-01/
│   │   │   ├── Antananarivo.csv
│   │   │   ├── Paris.csv
│   │   │   └── ...
│   │   ├── 2025-07-02/
│   │   │   └── ...
│   │   └── ...
│   ├── processed/
│   │   └── meteo_global.csv
│   └── star_schema/
│       ├── dim_city.csv
│       ├── dim_date.csv
│       ├── dim_meteo.csv
│       └── fact_weather.csv
│
├── historical-data/
│   └── raw/
│       └── {date}/city.csv            # Same as /data/raw but historical
│
├── .env                                # Environment variable file
├── requirements.txt
└── README.md
```

---

## Setup & Configuration

### 1. Environment Setup

```bash
git clone <repo-url>
cd AIRFLOW-WEATHER-FETCHING-ETL
python3 -m venv .venv
source .venv/bin/activate
echo 'export AIRFLOW_HOME="$(pwd)/airflow"' >> .venv/bin/activate
pip install -r requirements.txt
```

### 2. Airflow Initialization

```bash
airflow db init
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password your_password
airflow db reset
airflow db migrate
```

### 3. `airflow.cfg` Configuration

Update the following values in `airflow/airflow.cfg`:

| Setting                                 | Value                                                            |
|-----------------------------------------|------------------------------------------------------------------|
| `dags_folder`                           | `<your_path>/airflow/dags`                                       |
| `plugins_folder`                        | `<your_path>/airflow/plugins`                                    |
| `sql_alchemy_conn`                      | `sqlite:////<your_path>/airflow/airflow.db`                      |
| `base_log_folder`                       | `<your_path>/airflow/logs`                                       |
| `dag_processor_child_process_log_directory` | `<your_path>/airflow/logs/dag_processor`                  |
| `config_file`                           | `<your_path>/airflow/webserver_config.py`                        |
| `load_examples`                         | `False`                                                          |

---

## Required Environment Variables

### `.env` File

Create a `.env` file in your project root:

```ini
GOOGLE_SERVICE_ACCOUNT_JSON=<base64_encoded_credentials.json>
DRIVE_FOLDER_ID=<your_drive_folder_id>
```

### Airflow Variables

These must be registered using the CLI:

```bash
airflow variables set API_KEY <your_openweather_api_key>
airflow variables set DRIVE_FOLDER_ID <your_drive_folder_id>
airflow variables set GOOGLE_SERVICE_ACCOUNT_JSON "$(cat path/to/credentials.json)"
```

---

## Scripts Overview

### Historical Scripts (`/dags/historical-scripts/`)

One-time scripts to fetch and clean large datasets from **Open-Meteo**.

- `extract.py`: Fetch raw historical data by city and date range.
- `clean.py`, `merge.py`: Prepare and consolidate raw data.
- `load.py`: Upload raw files to Google Drive. (⚠️ Slow: ~18 files per day)

Set custom date ranges in `extract.py`:

```python
params = {
    "start_date": "2021-01-01",
    "end_date": "2021-12-31",
    ...
}
```

---

### Daily Scripts (`/dags/scripts/`)

Scripts scheduled by Airflow to run daily.

- `extract.py`: Fetches daily weather data from OpenWeatherMap.
- `transform.py`: Builds the star schema:
  - `dim_city`, `dim_date`, `dim_meteo`, `fact_weather`
- `load.py`: Uploads processed/star schema data to Google Drive.

---