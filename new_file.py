#!/usr/bin/env python3
"""
Weather App - Fetches weather data using OpenWeatherMap API
"""

import requests
import json
import os
import sys
from datetime import datetime


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


def save_raw_response(weather_data, city_name, output_dir="weather_data"):
    """
    Saves the raw JSON response to a file
    
    Args:
        weather_data (dict): Raw weather data from API
        city_name (str): Name of the city (for filename)
        output_dir (str): Directory to save the file (default: "weather_data")
    
    Returns:
        str: Path to the saved file, None if failed
    """
    if not weather_data:
        print("No weather data to save.")
        return None
    
    try:
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Sanitize city name for filename (remove spaces, special chars)
        safe_city_name = "".join(c for c in city_name if c.isalnum() or c in (' ', '-', '_')).strip().replace(' ', '_')
        filename = f"{safe_city_name}_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
        # Write JSON to file with pretty formatting
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(weather_data, f, indent=4, ensure_ascii=False)
        
        print(f"\n✓ Raw response saved to: {filepath}")
        return filepath
        
    except Exception as e:
        print(f"Error saving raw response to file: {e}")
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
        # Save raw response to file
        save_raw_response(weather_data, city_name)
        
        # Display formatted weather information
        display_weather(weather_data)
    else:
        print("Failed to retrieve weather data. Please check:")
        print("1. Your API key is correct")
        print("2. The city name is spelled correctly")
        print("3. You have an internet connection")


if __name__ == "__main__":
    main()

