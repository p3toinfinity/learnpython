# Setting Up Airflow Variables for Weather Pipeline

## Required Variables

Your weather pipeline DAG needs these Airflow Variables:

1. **OPENWEATHER_API_KEY** - Your OpenWeatherMap API key (required)
2. **WEATHER_CITY** - City name to fetch weather for (optional, defaults to "London")

## How to Set Variables

### Option 1: Via Airflow UI (Easiest)

1. Open Airflow UI: http://localhost:8080
2. Go to: **Admin → Variables**
3. Click **"+"** to add a new variable
4. Add these variables:

   **Variable 1:**
   - Key: `OPENWEATHER_API_KEY`
   - Value: `your_api_key_here`
   - Description: OpenWeatherMap API Key

   **Variable 2:**
   - Key: `WEATHER_CITY`
   - Value: `London` (or your preferred city)
   - Description: City name for weather data

5. Click **Save**

### Option 2: Via CLI

```bash
export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home

# Set API key
airflow variables set OPENWEATHER_API_KEY "your_api_key_here"

# Set city name
airflow variables set WEATHER_CITY "London"
```

### Option 3: Using the Setup Script

```bash
./setup_airflow_variables.sh
```

This will prompt you for the API key and city name.

## Verify Variables Are Set

```bash
export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home
airflow variables list
```

You should see:
- `OPENWEATHER_API_KEY`
- `WEATHER_CITY`

## Get Your OpenWeatherMap API Key

If you don't have an API key yet:

1. Go to: https://openweathermap.org/api
2. Sign up for a free account
3. Get your API key from the dashboard
4. Use it in the Airflow Variable above

## After Setting Variables

Once variables are set:
1. The DAG will automatically use them on the next run
2. You can trigger the DAG manually from the UI
3. Or wait for the scheduled run (every hour)

## Troubleshooting

**Variable not found error?**
- Make sure you set the variable in Airflow (not just environment variable)
- Check spelling: `OPENWEATHER_API_KEY` (case-sensitive)
- Verify with: `airflow variables list`

**Still getting errors?**
- Check the Airflow logs for detailed error messages
- Make sure Airflow is running: `./check_airflow.sh`

