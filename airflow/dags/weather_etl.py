from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
from scripts.extract import extract_forecast_data
from scripts.merge import merge_data
from scripts.transform import transform_to_star_schema
from scripts.load import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1)
}

CITIES = [
    "Antananarivo",  
    "London",        
    "Paris",         
    "Tokyo",         
    "Sydney",        
    "Reykjavik",     
    "Moscow",        
    "Cairo",         
    "Cape Town",     
    "Nairobi",       
    "Mumbai",         
    "Singapore",     
    "Dubai",         
    "New York",       
    "Mexico City",   
    "São Paulo",     
    "Buenos Aires",  
    "Auckland"
]

with DAG(
    'weather_forecast_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=2
) as dag:
    
    extract_task = [
        PythonOperator(
            task_id=f'{city.lower().replace(" ", "_")}',
            python_callable=extract_forecast_data,
            op_args=[city, "{{var.value.API_KEY}}", "{{ds}}"],
        )
        for city in CITIES
    ]
    
    merge_task = PythonOperator(
        task_id='merge_files',
        python_callable=merge_data,
        op_args=["{{ds}}"]
    )
    
    transform_task = PythonOperator(
        task_id="transform_to_star_schema",
        python_callable=transform_to_star_schema,
    )
    
    load_task = PythonOperator(
        task_id='load_to_drive',
        python_callable=main,
        op_args=["{{ var.value.GOOGLE_SERVICE_ACCOUNT_JSON }}", "{{ var.value.DRIVE_FOLDER_ID }}"],
    )
    
    extract_task >> merge_task >> transform_task >> load_task  # type: ignore