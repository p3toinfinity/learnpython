#!/bin/bash
# Setup script for Airflow environment variables

# Set Airflow home directory
export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home

# Set OpenWeatherMap API key (if not already set)
# export OPENWEATHER_API_KEY="your_api_key_here"

echo "Airflow environment variables set:"
echo "  AIRFLOW_HOME=$AIRFLOW_HOME"
echo ""
echo "To use these variables in your current shell, run:"
echo "  source setup_airflow_env.sh"
echo ""
echo "Or add to your ~/.zshrc:"
echo "  export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home"
echo "  export OPENWEATHER_API_KEY=\"your_api_key_here\""

