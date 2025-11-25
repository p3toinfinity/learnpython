#!/bin/bash
# Script to set up Airflow Variables for weather pipeline

export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home

echo "🔧 Setting up Airflow Variables for Weather Pipeline"
echo ""

# Check if OPENWEATHER_API_KEY is set in environment
if [ -z "$OPENWEATHER_API_KEY" ]; then
    echo "⚠️  OPENWEATHER_API_KEY not found in environment"
    read -p "Enter your OpenWeatherMap API key: " api_key
    if [ -z "$api_key" ]; then
        echo "❌ API key is required. Exiting."
        exit 1
    fi
    export OPENWEATHER_API_KEY="$api_key"
else
    api_key="$OPENWEATHER_API_KEY"
    echo "✓ Found OPENWEATHER_API_KEY in environment"
fi

# Set Airflow Variables
echo ""
echo "Setting Airflow Variables..."

airflow variables set OPENWEATHER_API_KEY "$api_key"
echo "✓ Set OPENWEATHER_API_KEY"

# Set city name (optional)
read -p "Enter city name (default: London): " city_name
city_name=${city_name:-London}
airflow variables set WEATHER_CITY "$city_name"
echo "✓ Set WEATHER_CITY to: $city_name"

echo ""
echo "✅ Setup complete!"
echo ""
echo "You can view variables in Airflow UI: Admin → Variables"
echo "Or via CLI: airflow variables list"

