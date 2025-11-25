#!/bin/bash
# Script to stop all Airflow processes

echo "🛑 Stopping Airflow processes..."

# Find and kill all Airflow processes
pkill -f "airflow.*standalone" 2>/dev/null
pkill -f "airflow.*scheduler" 2>/dev/null
pkill -f "airflow.*api-server" 2>/dev/null
pkill -f "airflow.*dag-processor" 2>/dev/null
pkill -f "airflow.*triggerer" 2>/dev/null

# Wait a moment
sleep 2

# Check if any processes are still running
if pgrep -f airflow > /dev/null; then
    echo "⚠️  Some Airflow processes may still be running:"
    ps aux | grep airflow | grep -v grep
    echo ""
    echo "To force kill, run: pkill -9 -f airflow"
else
    echo "✅ All Airflow processes stopped"
    echo "   You can now start Airflow again with: ./start_airflow.sh"
fi

