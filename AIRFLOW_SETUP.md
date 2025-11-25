# Airflow Setup Guide

## ✅ Installation Complete

Airflow 3.1.3 has been successfully installed and configured!

## Directory Structure

```
learnpython/
├── dags/                          # Your DAG files go here
├── airflow_home/                  # Airflow home directory
│   ├── airflow.cfg               # Airflow configuration
│   ├── airflow.db                # SQLite database (created)
│   └── logs/                      # Airflow logs
└── setup_airflow_env.sh          # Environment setup script
```

## Environment Variables

Before running Airflow commands, you need to set the `AIRFLOW_HOME` environment variable:

```bash
export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home
```

Or use the setup script:
```bash
source setup_airflow_env.sh
```

**Recommended:** Add to your `~/.zshrc` or `~/.bashrc`:
```bash
export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home
export OPENWEATHER_API_KEY="your_api_key_here"  # Optional, for weather DAG
```

Then reload:
```bash
source ~/.zshrc
```

## 🚀 Quick Start Scripts

**Easiest way to start/stop Airflow:**

```bash
# Start Airflow
./start_airflow.sh

# Check status and get login info
./check_airflow.sh

# Stop Airflow
./stop_airflow.sh
```

## ⏱️ How Long Does Airflow Run?

**The current Airflow instance will run until:**
- ✅ You manually stop it (Ctrl+C or `./stop_airflow.sh`)
- ✅ You close the terminal window (if started in foreground)
- ✅ Your computer shuts down or restarts
- ✅ The process is killed (system crash, out of memory, etc.)

**Airflow does NOT automatically stop** - it's designed to run continuously as a service. You need to explicitly stop it when you're done.

**Best Practice:**
- Start Airflow when you need it: `./start_airflow.sh`
- Stop Airflow when done: `./stop_airflow.sh`
- Check status anytime: `./check_airflow.sh`

## Starting Airflow

### Option 1: Standalone Mode (Recommended for first-time setup)

This starts both the webserver and scheduler together and creates a default admin user:

```bash
export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home
airflow standalone
```

This will:
- Start the webserver on http://localhost:8080
- Start the scheduler
- Create a default admin user (username: `admin`, password will be displayed in the terminal)

### Option 2: Separate Processes

**Terminal 1 - Start Scheduler:**
```bash
export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home
airflow scheduler
```

**Terminal 2 - Start Webserver:**
```bash
export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home
airflow api-server --port 8080
```

## Accessing Airflow UI

1. Open your browser: http://localhost:8080
2. Login credentials:
   - **Username:** `admin`
   - **Password:** Check the terminal output when you start Airflow (in standalone mode) or check `airflow_home/simple_auth_manager_passwords.json.generated`

## Configuration

### DAGs Folder
- **Location:** `/Users/karthikdhina/PycharmProjects/learnpython/dags`
- Already configured in `airflow.cfg`

### Example DAGs
- **Disabled** (set `load_examples = False` in config)

### Database
- **Type:** SQLite (default for development)
- **Location:** `airflow_home/airflow.db`

## Useful Commands

```bash
# Set environment variable
export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home

# Check Airflow version
airflow version

# List DAGs
airflow dags list

# Test a DAG
airflow dags test <dag_id> <execution_date>

# Check DAG syntax
airflow dags list-import-errors

# View configuration
airflow config get-value core dags_folder

# Check database status
airflow db check
```

## Next Steps

1. **Set Environment Variables** (if not already done):
   ```bash
   export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home
   export OPENWEATHER_API_KEY="your_api_key_here"
   ```

2. **Start Airflow:**
   ```bash
   airflow standalone
   ```

3. **Access the UI:** http://localhost:8080

4. **Create your first DAG** in the `dags/` directory

## Troubleshooting

### Issue: "AIRFLOW_HOME not set"
**Solution:** Export the environment variable:
```bash
export AIRFLOW_HOME=/Users/karthikdhina/PycharmProjects/learnpython/airflow_home
```

### Issue: "DAGs not showing up"
**Solution:** 
- Check that DAGs are in `/Users/karthikdhina/PycharmProjects/learnpython/dags`
- Check for import errors: `airflow dags list-import-errors`
- Ensure the scheduler is running

### Issue: "Port 8080 already in use"
**Solution:** Use a different port:
```bash
airflow api-server --port 8081
```

## Notes

- Airflow 3.x uses SimpleAuthManager (simpler than previous versions)
- Default admin user is configured in `airflow.cfg` as `admin:admin`
- Password is stored in `airflow_home/simple_auth_manager_passwords.json.generated`
- For production, consider using PostgreSQL instead of SQLite

