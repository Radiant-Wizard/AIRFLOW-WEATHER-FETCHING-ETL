# AIRFLOW-WEATHER-FETCHING-ETL
This is a complete apache airflow etl pipeline to fetch, transform and load data from openweathermapapi
- **Python Version**: `3.10.12`
- **Airflow version**: `3.0.2`
- **For those using a venv**: run `echo 'export AIRFLOW_HOME="$(pwd)/airflow"' >> .venv/bin/activate` into the root directory of the project which contains the venv to automatically set the AIRFLOW_HOME when activating the venv
- `Package used`: 
    - google-api-python-client 
    - google-auth-httplib2 
    - google-auth-oauthlib 
    - pandas 
    - requests