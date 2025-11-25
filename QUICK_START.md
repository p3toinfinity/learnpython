# Airflow Quick Start Guide

## 🚀 Starting Airflow (Every Time)

**Simple one-command start:**
```bash
./start_airflow.sh
```

This will:
- ✅ Check if Airflow is already running
- ✅ Start webserver + scheduler
- ✅ Show you the UI URL and login info

**Then open:** http://localhost:8080

---

## 🛑 Stopping Airflow

**Simple one-command stop:**
```bash
./stop_airflow.sh
```

Or press `Ctrl+C` in the terminal where Airflow is running.

---

## 📊 Check Status

**See if Airflow is running and get login credentials:**
```bash
./check_airflow.sh
```

---

## ⏱️ How Long Does It Run?

**Airflow runs until you stop it:**
- It will **NOT** stop automatically
- It runs until you:
  - Run `./stop_airflow.sh`
  - Press `Ctrl+C` in the terminal
  - Close the terminal (if started in foreground)
  - Shut down your computer

**Think of it like a web server** - it stays running until you tell it to stop.

---

## 📝 Daily Workflow

1. **Start Airflow:**
   ```bash
   ./start_airflow.sh
   ```

2. **Open UI:** http://localhost:8080
   - Username: `admin`
   - Password: Check with `./check_airflow.sh`

3. **Work with your DAGs**

4. **Stop when done:**
   ```bash
   ./stop_airflow.sh
   ```

---

## 🔑 Getting Login Password

The password is auto-generated and stored in:
```
airflow_home/simple_auth_manager_passwords.json.generated
```

**Quick way to get it:**
```bash
./check_airflow.sh
```

Or manually:
```bash
cat airflow_home/simple_auth_manager_passwords.json.generated
```

---

## 🆘 Troubleshooting

**Airflow won't start?**
- Check if already running: `./check_airflow.sh`
- Kill existing processes: `./stop_airflow.sh`
- Try again: `./start_airflow.sh`

**Port 8080 already in use?**
- Something else is using port 8080
- Stop it or change Airflow port in `airflow_home/airflow.cfg`

**Can't access UI?**
- Wait 10-15 seconds after starting
- Check status: `./check_airflow.sh`
- Try: `curl http://localhost:8080/api/v2/monitor/health`

