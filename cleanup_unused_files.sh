#!/bin/bash
# Script to clean up unused files and directories

echo "🧹 Cleaning up unused files..."
echo ""

# 1. Remove Python cache directories
echo "1. Removing Python cache directories..."
find . -type d -name "__pycache__" -not -path "./.venv/*" -exec rm -rf {} + 2>/dev/null
echo "   ✓ Removed __pycache__ directories"

# 2. Remove old unused script
if [ -f "load_to_snowflake.py" ]; then
    echo "2. Removing old load_to_snowflake.py (replaced by load_to_snowflake_pandas.py)..."
    rm -f load_to_snowflake.py
    echo "   ✓ Removed load_to_snowflake.py"
else
    echo "2. load_to_snowflake.py not found (already removed)"
fi

# 3. Remove old local weather data file
if [ -f "weather_data/Madurai_20251118_090205.json" ]; then
    echo "3. Removing old local weather data file (data is now in S3)..."
    rm -f weather_data/Madurai_20251118_090205.json
    echo "   ✓ Removed old weather data file"
else
    echo "3. Old weather data file not found"
fi

# 4. Optional: Remove test script (commented out - uncomment if you want to delete)
# if [ -f "test_aws_connection.py" ]; then
#     echo "4. Removing test_aws_connection.py..."
#     rm -f test_aws_connection.py
#     echo "   ✓ Removed test_aws_connection.py"
# fi

# 5. Optional: Remove tutorials directory (commented out - uncomment if you want to delete)
# if [ -d "tutorials" ]; then
#     echo "5. Removing tutorials directory..."
#     rm -rf tutorials
#     echo "   ✓ Removed tutorials directory"
# fi

echo ""
echo "✅ Cleanup complete!"
echo ""
echo "Note: The following were NOT deleted (uncomment in script if you want to remove):"
echo "  - test_aws_connection.py (test script)"
echo "  - tutorials/ (tutorial files)"
echo ""
echo "To clean old Airflow logs manually:"
echo "  rm -rf airflow_home/logs/dag_id=weather_data_pipeline/run_id=*"

