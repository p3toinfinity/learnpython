#!/usr/bin/env python3
"""
Weather App - Fetches weather data using OpenWeatherMap API
"""

import requests
import json
import os
import sys
from datetime import datetime
from pathlib import Path
import boto3
from botocore.exceptions import ClientError, NoCredentialsError


def load_aws_config(config_path="aws_config.json"):
    """
    Loads AWS configuration from a JSON file
    
    Args:
        config_path (str): Path to the AWS config JSON file
        
    Returns:
        dict: AWS configuration dictionary, None if failed
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        print(f"Error: AWS config file '{config_path}' not found.")
        return None
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        aws_config = config.get('aws', {})
        required_fields = ['access_key_id', 'secret_access_key', 'bucket_name']
        
        if not all(field in aws_config and aws_config[field] for field in required_fields):
            print(f"Error: Missing required fields in AWS config. Required: {required_fields}")
            return None
        
        print(f"\n✓ Loaded AWS configuration from: {config_path}")
        return aws_config
        
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in config file: {e}")
        return None
    except Exception as e:
        print(f"Error loading AWS config: {e}")
        return None


def get_weather(city_name, api_key):
    """
    Fetches weather data for a given city using OpenWeatherMap API
    
    Args:
        city_name (str): Name of the city
        api_key (str): OpenWeatherMap API key
        
    Returns:
        dict: Weather data if successful, None otherwise
    """
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    
    params = {
        "q": city_name,
        "appid": api_key,
        "units": "metric"  # Use metric units (Celsius)
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None


def save_raw_response_to_s3(weather_data, city_name, aws_config):
    """
    Saves the raw JSON response to AWS S3
    
    Args:
        weather_data (dict): Raw weather data from API
        city_name (str): Name of the city (for filename)
        aws_config (dict): AWS configuration dictionary containing:
            - access_key_id: AWS access key ID
            - secret_access_key: AWS secret access key
            - region: AWS region (optional, defaults to us-east-1)
            - bucket_name: S3 bucket name
            - s3_prefix: Optional S3 prefix/folder path (optional)
    
    Returns:
        str: S3 object key (path) if successful, None if failed
    """
    if not weather_data:
        print("No weather data to save.")
        return None
    
    try:
        # Initialize S3 client with credentials from config
        region = aws_config.get('region', 'us-east-1')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_config['access_key_id'],
            aws_secret_access_key=aws_config['secret_access_key'],
            region_name=region
        )
        
        bucket_name = aws_config['bucket_name']
        s3_prefix = aws_config.get('s3_prefix', '').strip()
        
        # Verify credentials by attempting to get caller identity (using STS)
        try:
            sts_client = boto3.client(
                'sts',
                aws_access_key_id=aws_config['access_key_id'],
                aws_secret_access_key=aws_config['secret_access_key'],
                region_name=region
            )
            identity = sts_client.get_caller_identity()
            print(f"✓ AWS credentials verified. Account: {identity.get('Account', 'N/A')}")
        except Exception as cred_error:
            print(f"⚠ Warning: Could not verify AWS credentials: {cred_error}")
            print("   Continuing with S3 upload attempt...")
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Sanitize city name for filename (remove spaces, special chars)
        safe_city_name = "".join(c for c in city_name if c.isalnum() or c in (' ', '-', '_')).strip().replace(' ', '_')
        filename = f"{safe_city_name}_{timestamp}.json"
        
        # Construct S3 object key (path)
        if s3_prefix:
            # Remove leading/trailing slashes and ensure single separator
            s3_prefix = s3_prefix.strip('/')
            s3_key = f"{s3_prefix}/{filename}" if s3_prefix else filename
        else:
            s3_key = filename
        
        print(f"Uploading to S3:")
        print(f"  Bucket: {bucket_name}")
        print(f"  Key: {s3_key}")
        print(f"  Region: {aws_config.get('region', 'us-east-1')}")
        
        # Convert JSON to string
        json_content = json.dumps(weather_data, indent=4, ensure_ascii=False)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_content.encode('utf-8'),
            ContentType='application/json'
        )
        
        s3_url = f"s3://{bucket_name}/{s3_key}"
        print(f"\n✓ Raw response saved to S3: {s3_url}")
        return s3_key
        
    except NoCredentialsError:
        print("Error: AWS credentials invalid or not found.")
        print("Please check your AWS credentials in 'aws_config.json'.")
        return None
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', str(e))
        
        print(f"\n✗ AWS S3 Error ({error_code}): {error_message}")
        
        if error_code == 'NoSuchBucket':
            print(f"   Bucket name: '{bucket_name}'")
            print(f"   Please verify the bucket name exists in region '{aws_config.get('region', 'us-east-1')}'")
        elif error_code == 'AccessDenied':
            print(f"   Bucket: '{bucket_name}'")
            print(f"   Your AWS credentials are valid, but you don't have permission to access this bucket.")
            print(f"   Please check:")
            print(f"   1. The IAM user has S3 permissions (e.g., s3:PutObject, s3:GetObject)")
            print(f"   2. The bucket name is correct: '{bucket_name}'")
            print(f"   3. The bucket exists in region '{aws_config.get('region', 'us-east-1')}'")
            print(f"   4. There are no bucket policies blocking your access")
        elif error_code == 'InvalidAccessKeyId':
            print(f"   The AWS Access Key ID provided is invalid.")
            print(f"   Please check your 'access_key_id' in aws_config.json")
        elif error_code == 'SignatureDoesNotMatch':
            print(f"   The AWS Secret Access Key provided is invalid.")
            print(f"   Please check your 'secret_access_key' in aws_config.json")
        else:
            print(f"   Full error details: {e}")
        return None
    except Exception as e:
        print(f"\n✗ Unexpected error saving to S3: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None


def display_weather(weather_data):
    """
    Displays weather information in a readable format
    
    Args:
        weather_data (dict): Weather data from API
    """
    if not weather_data:
        print("No weather data to display.")
        return
    
    try:
        city = weather_data["name"]
        country = weather_data["sys"]["country"]
        temp = weather_data["main"]["temp"]
        feels_like = weather_data["main"]["feels_like"]
        humidity = weather_data["main"]["humidity"]
        description = weather_data["weather"][0]["description"].title()
        wind_speed = weather_data.get("wind", {}).get("speed", "N/A")
        
        print("\n" + "="*50)
        print(f"Weather in {city}, {country}")
        print("="*50)
        print(f"Temperature: {temp}°C (feels like {feels_like}°C)")
        print(f"Condition: {description}")
        print(f"Humidity: {humidity}%")
        print(f"Wind Speed: {wind_speed} m/s")
        print("="*50 + "\n")
    except KeyError as e:
        print(f"Error parsing weather data: Missing key {e}")


def main():
    """
    Main function to run the weather app
    """
    # Load AWS configuration from JSON file
    aws_config = load_aws_config()
    if not aws_config:
        print("\nPlease create 'aws_config.json' with the following structure:")
        print("""
{
  "aws": {
    "access_key_id": "your_access_key_id",
    "secret_access_key": "your_secret_access_key",
    "region": "us-east-1",
    "bucket_name": "your-bucket-name",
    "s3_prefix": "weather-data"
  }
}
        """)
        sys.exit(1)
    
    # Get API key from environment variable or use a placeholder
    api_key = os.getenv("OPENWEATHER_API_KEY")
    
    if not api_key:
        print("Warning: OPENWEATHER_API_KEY environment variable not set.")
        print("Please set it using: export OPENWEATHER_API_KEY='your_api_key'")
        print("\nYou can get a free API key from: https://openweathermap.org/api")
        api_key = input("\nEnter your API key (or press Enter to exit): ").strip()
        if not api_key:
            print("Exiting...")
            sys.exit(1)
    
    # Get city name from user
    city_name = input("Enter city name: ").strip()
    
    if not city_name:
        print("City name cannot be empty.")
        sys.exit(1)
    
    print(f"\nFetching weather data for {city_name}...")
    weather_data = get_weather(city_name, api_key)
    
    if weather_data:
        # Save raw response to S3
        save_raw_response_to_s3(weather_data, city_name, aws_config)
        
        # Display formatted weather information
        display_weather(weather_data)
    else:
        print("Failed to retrieve weather data. Please check:")
        print("1. Your API key is correct")
        print("2. The city name is spelled correctly")
        print("3. You have an internet connection")


if __name__ == "__main__":
    main()

