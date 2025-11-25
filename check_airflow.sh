#!/bin/bash
# Script to check Airflow status

export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home

echo "📊 Airflow Status Check"
echo "======================"
echo ""

# Check if processes are running
if pgrep -f "airflow.*scheduler" > /dev/null; then
    echo "✅ Scheduler: Running"
else
    echo "❌ Scheduler: Not running"
fi

if pgrep -f "airflow.*api-server" > /dev/null; then
    echo "✅ Webserver: Running"
else
    echo "❌ Webserver: Not running"
fi

if pgrep -f "airflow.*dag-processor" > /dev/null; then
    echo "✅ DAG Processor: Running"
else
    echo "❌ DAG Processor: Not running"
fi

echo ""

# Check if UI is accessible
if curl -s http://localhost:8080/api/v2/monitor/health > /dev/null 2>&1; then
    echo "✅ UI Status: Accessible at http://localhost:8080"
    
    # Get password if available
    if [ -f "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated" ]; then
        PASSWORD=$(python3 -c "import json; print(json.load(open('$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated'))['admin'])" 2>/dev/null)
        if [ ! -z "$PASSWORD" ]; then
            echo "   Username: admin"
            echo "   Password: $PASSWORD"
        fi
    fi
else
    echo "❌ UI Status: Not accessible"
fi

echo ""
echo "Processes:"
ps aux | grep airflow | grep -v grep | head -5 || echo "   No Airflow processes found"

