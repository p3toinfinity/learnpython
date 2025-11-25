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
    """Task to fetch weather data for multiple cities and save to S3"""
    import json
    from weather_to_json import load_aws_config, get_weather, save_raw_response_to_s3
    from airflow.sdk import Variable
    
    # Get API key from Airflow Variable
    try:
        api_key = Variable.get("OPENWEATHER_API_KEY")
    except:
        api_key = os.getenv("OPENWEATHER_API_KEY")
    
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY not set. Please set it as an Airflow Variable or environment variable.")
    
    # Load cities from JSON file
    cities_file = project_root / "cities.json"
    if not cities_file.exists():
        raise FileNotFoundError(f"Cities file not found: {cities_file}")
    
    with open(cities_file, 'r', encoding='utf-8') as f:
        cities_data = json.load(f)
    
    cities = cities_data.get('cities', [])
    if not cities:
        raise ValueError("No cities found in cities.json file")
    
    print(f"✓ Loaded {len(cities)} cities from cities.json")
    
    # Load AWS config
    aws_config = load_aws_config()
    if not aws_config:
        raise ValueError("Failed to load AWS configuration")
    
    # Fetch weather data for each city
    successful = 0
    failed = 0
    s3_keys = []
    
    for city_name in cities:
        try:
            print(f"\nFetching weather data for {city_name}...")
            weather_data = get_weather(city_name, api_key)
            
            if weather_data:
                s3_key = save_raw_response_to_s3(weather_data, city_name, aws_config)
                if s3_key:
                    s3_keys.append(s3_key)
                    successful += 1
                    print(f"✓ Weather data saved to S3: {s3_key}")
                else:
                    failed += 1
                    print(f"✗ Failed to save weather data for {city_name}")
            else:
                failed += 1
                print(f"✗ Failed to fetch weather data for {city_name}")
        except Exception as e:
            failed += 1
            print(f"✗ Error processing {city_name}: {e}")
            continue
    
    print(f"\n{'='*70}")
    print(f"Summary: {successful} successful, {failed} failed out of {len(cities)} cities")
    print(f"{'='*70}")
    
    if successful == 0:
        raise ValueError("No weather data was successfully fetched and saved")
    
    return s3_keys


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

