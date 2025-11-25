"""
Weather Data Pipeline DAG
Fetches weather data and loads to Snowflake
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Fetch weather data and load to Snowflake',
    schedule=timedelta(hours=1),  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['weather', 'snowflake', 'etl'],
)


def fetch_weather_task(**context):
    """Task to fetch weather data and save to S3"""
    from weather_to_json import load_aws_config, get_weather, save_raw_response_to_s3
    
    # Get city name from Airflow Variable or default
    from airflow.models import Variable
    city_name = Variable.get("WEATHER_CITY", default_var="London")
    api_key = os.getenv("OPENWEATHER_API_KEY")
    
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY environment variable not set")
    
    # Load AWS config
    aws_config = load_aws_config()
    if not aws_config:
        raise ValueError("Failed to load AWS configuration")
    
    # Fetch weather data
    weather_data = get_weather(city_name, api_key)
    if not weather_data:
        raise ValueError(f"Failed to fetch weather data for {city_name}")
    
    # Save to S3
    s3_key = save_raw_response_to_s3(weather_data, city_name, aws_config)
    if not s3_key:
        raise ValueError("Failed to save weather data to S3")
    
    print(f"✓ Weather data saved to S3: {s3_key}")
    return s3_key


def load_to_snowflake_task(**context):
    """Task to load weather data from S3 to Snowflake"""
    from load_to_snowflake_pandas import WeatherDataLoader
    
    loader = WeatherDataLoader()
    loader.run(load_raw=True, load_normalized=True)
    
    print("✓ Data loaded to Snowflake successfully")


# Define tasks
fetch_weather = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_task,
    dag=dag,
)

load_snowflake = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake_task,
    dag=dag,
)

# Set task dependencies
fetch_weather >> load_snowflake

