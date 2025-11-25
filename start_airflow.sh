#!/bin/bash
# Script to start Airflow in standalone mode

# Set Airflow home directory
export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home

# Check if Airflow is already running
if pgrep -f "airflow.*standalone" > /dev/null || pgrep -f "airflow.*scheduler" > /dev/null; then
    echo "⚠️  Airflow appears to be already running!"
    echo "   Check processes: ps aux | grep airflow"
    echo "   Or check the UI: http://localhost:8080"
    echo ""
    read -p "Do you want to start anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "🚀 Starting Airflow..."
echo "   AIRFLOW_HOME: $AIRFLOW_HOME"
echo "   UI will be available at: http://localhost:8080"
echo ""
echo "   To stop Airflow, press Ctrl+C or run: ./stop_airflow.sh"
echo ""

# Start Airflow in standalone mode
cd /Users/karthikdhina/PycharmProjects/learnpython
airflow standalone

