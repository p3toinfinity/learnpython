#!/usr/bin/env python3

import json
import os
import sys
from pathlib import Path
from datetime import datetime
import snowflake.connector as snowflake

def read_json_files(directory="weather_data"):
    json_files = []
    weather_dir = Path(directory)
    
    if not weather_dir.exists():
        print(f"Error: Directory '{directory}' not found")
        return []
    
    for json_file in weather_dir.glob("*.json"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                json_files.append({
                    'filename': json_file.name,
                    'data': data,
                    'filepath': str(json_file)
                })
            print(f"✓ Read: {json_file.name}")
        except json.JSONDecodeError as e:
            print(f"✗ JSON parsing error in {json_file.name}: {e}")
        except Exception as e:
            print(f"✗ Error reading {json_file.name}: {e}")
    
    return json_files


def load_config_file(config_path="snowflake_config.json"):
    config_file = Path(config_path)
    
    if not config_file.exists():
        return None
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        snowflake_config = config.get('snowflake', {})
        required_fields = ['account', 'user', 'password', 'warehouse', 'database', 'schema']
        if all(field in snowflake_config and snowflake_config[field] for field in required_fields):
            print(f"\n✓ Found Snowflake credentials in config file: {config_path}")
            return {
                'account': snowflake_config['account'],
                'user': snowflake_config['user'],
                'password': snowflake_config['password'],
                'warehouse': snowflake_config['warehouse'],
                'database': snowflake_config['database'],
                'schema': snowflake_config['schema']
            }
        else:
            missing_fields = [field for field in required_fields if not snowflake_config.get(field)]
            print(f"\n⚠ Config file found but missing required fields: {', '.join(missing_fields)}")
            return None
    except json.JSONDecodeError as e:
        print(f"\n✗ Error parsing config file {config_path}: {e}")
        print("   Please check that the file contains valid JSON.")
        return None
    except Exception as e:
        print(f"\n✗ Error reading config file {config_path}: {e}")
        return None


def get_snowflake_credentials():
    config_creds = load_config_file("snowflake_config.json")
    if config_creds:
        return config_creds
    
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    
    if all([account, user, password, warehouse, database, schema]):
        print("\n✓ Found Snowflake credentials in environment variables")
        return {
            'account': account,
            'user': user,
            'password': password,
            'warehouse': warehouse,
            'database': database,
            'schema': schema
        }
    
    print("\n⚠ No Snowflake credentials found.")
    print("   Please create 'snowflake_config.json' using 'snowflake_config.json.example' as a template,")
    print("   or set the following environment variables:")
    print("   - SNOWFLAKE_ACCOUNT")
    print("   - SNOWFLAKE_USER")
    print("   - SNOWFLAKE_PASSWORD")
    print("   - SNOWFLAKE_WAREHOUSE")
    print("   - SNOWFLAKE_DATABASE")
    print("   - SNOWFLAKE_SCHEMA")
    return None


def connect_to_snowflake(account, user, password, warehouse, database, schema):
    account_locator = account.split('-')[0] if '-' in account else account
    
    account_variations = [
        account_locator,
        f"{account_locator}.us-east-1",
        f"{account_locator}.us-west-2",
        f"{account_locator}.eu-west-1",
        account,
    ]
    
    for acc in account_variations:
        try:
            print(f"\nTrying to connect with account: {acc}...")
            print(f"User: {user}")
            
            conn = snowflake.connect(
                account=acc,
                user=user,
                password=password,
                warehouse=warehouse,
                database=database,
                schema=schema
            )
            print("✓ Connected to Snowflake successfully!")
            return conn
        except snowflake.errors.DatabaseError as e:
            error_msg = str(e)
            error_code = getattr(e, 'errno', None)
            if error_code == 250001 or "authentication" in error_msg.lower() or "password" in error_msg.lower() or "login" in error_msg.lower():
                if acc == account_variations[-1]:
                    print(f"\n✗ Authentication failed (Error 250001)!")
                    print(f"   This means the account format is correct, but username/password is wrong.")
                    print(f"   Error details: {error_msg}")
                    print(f"\n   Please verify in Snowflake web UI:")
                    print(f"   1. Username: '{user}' (exact case - Snowflake is case-sensitive)")
                    print(f"   2. Password is correct (no extra spaces)")
                    print(f"   3. User account is active and not locked")
                    print(f"   4. Try logging into Snowflake web UI with these exact credentials")
                    print(f"\n   Common issues:")
                    print(f"   - Username might be email format (e.g., 'user@domain.com')")
                    print(f"   - Username might need domain prefix (e.g., 'DOMAIN.USERNAME')")
                    print(f"   - Password special characters might need different encoding")
                continue
            else:
                print(f"✗ Database error: {e}")
                return None
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            if "404" in error_msg or "HttpError" in error_type:
                if acc == account_variations[-1]:
                    print(f"✗ Account not found (404 error)!")
                    print(f"   Tried account formats: {', '.join(account_variations)}")
                    print(f"   Please check your Snowflake account identifier.")
                    print(f"   It should be just the account locator (e.g., 'IQWKLHO')")
                    print(f"   or account locator with region (e.g., 'IQWKLHO.us-east-1')")
                continue
            elif "250001" in error_msg or "authentication" in error_msg.lower():
                if acc == account_variations[-1]:
                    print(f"✗ Authentication failed!")
                    print(f"   Error: {error_msg}")
                    print(f"   Please verify your credentials in snowflake_config.json")
                continue
            else:
                if acc == account_variations[-1]:
                    print(f"✗ Error connecting to Snowflake: {e}")
                    print(f"   Error type: {error_type}")
                continue
    
    return None


def load_data_to_variant_table(conn, json_files):
    cursor = conn.cursor()
    try:
        print(f"\nLoading {len(json_files)} record(s) into WEATHER_DATA_RAW table...")
        
        cursor.execute("SELECT COUNT(*) FROM WEATHER_DATA_RAW")
        existing_count = cursor.fetchone()[0]
        print(f"   Current records in table: {existing_count}")
        
        for i, item in enumerate(json_files, 1):
            try:
                data = item['data']
                city_name = data.get('name', 'Unknown')
                city_id = data.get('id', 0)
                country_code = data.get('sys', {}).get('country', '')
                
                json_str = json.dumps(data)
                json_str_escaped = json_str.replace("'", "''").replace("\\", "\\\\")
                
                city_name_escaped = city_name.replace("'", "''")
                country_code_escaped = country_code.replace("'", "''")
                
                sql = f"""
                    INSERT INTO WEATHER_DATA_RAW (CITY_NAME, CITY_ID, COUNTRY_CODE, WEATHER_JSON)
                    SELECT '{city_name_escaped}', {city_id}, '{country_code_escaped}', PARSE_JSON('{json_str_escaped}')
                """
                cursor.execute(sql)
                
                if i % 5 == 0:
                    print(f"   Progress: {i}/{len(json_files)} records processed...")
            except Exception as e:
                print(f"✗ Error inserting record {i} ({item.get('filename', 'unknown')}): {e}")
                print(f"   Error type: {type(e).__name__}")
                import traceback
                traceback.print_exc()
                raise
        
        conn.commit()
        print(f"✓ Successfully loaded {len(json_files)} record(s) into WEATHER_DATA_RAW")
        return True
    except snowflake.errors.ProgrammingError as e:
        print(f"✗ SQL execution error: {e}")
        print(f"   Error code: {e.errno if hasattr(e, 'errno') else 'N/A'}")
        print(f"   SQL state: {e.sqlstate if hasattr(e, 'sqlstate') else 'N/A'}")
        conn.rollback()
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        cursor.close()


def load_data_to_normalized_table(conn, json_files):
    cursor = conn.cursor()
    try:
        print(f"\nLoading {len(json_files)} record(s) into WEATHER_DATA_NORMALIZED table...")
        
        for i, item in enumerate(json_files, 1):
            try:
                data = item['data']
                coord = data.get('coord', {})
                weather_array = data.get('weather', [{}])
                weather = weather_array[0] if weather_array else {}
                main_data = data.get('main', {})
                wind = data.get('wind', {})
                clouds = data.get('clouds', {})
                sys_data = data.get('sys', {})
                
                name = data.get("name", "").replace("'", "''")
                country = sys_data.get("country", "").replace("'", "''")
                weather_main = weather.get("main", "").replace("'", "''")
                weather_desc = weather.get("description", "").replace("'", "''")
                weather_icon = weather.get("icon", "").replace("'", "''")
                base = data.get("base", "").replace("'", "''")
                
                sql = f"""
                    INSERT INTO WEATHER_DATA_NORMALIZED (
                        CITY_NAME, CITY_ID, COUNTRY_CODE,
                        LONGITUDE, LATITUDE,
                        WEATHER_ID, WEATHER_MAIN, WEATHER_DESCRIPTION, WEATHER_ICON,
                        BASE,
                        TEMPERATURE, FEELS_LIKE, TEMP_MIN, TEMP_MAX,
                        PRESSURE, HUMIDITY, SEA_LEVEL, GROUND_LEVEL,
                        VISIBILITY,
                        WIND_SPEED, WIND_DEGREE,
                        CLOUD_COVERAGE,
                        DATA_TIMESTAMP, SUNRISE_TIMESTAMP, SUNSET_TIMESTAMP, TIMEZONE_OFFSET,
                        SYS_TYPE, SYS_ID,
                        RESPONSE_CODE
                    ) VALUES (
                        '{name}', {data.get("id", 0)}, '{country}',
                        {coord.get("lon", 0)}, {coord.get("lat", 0)},
                        {weather.get("id", 0)}, '{weather_main}', '{weather_desc}', '{weather_icon}',
                        '{base}',
                        {main_data.get("temp", 0)}, {main_data.get("feels_like", 0)}, {main_data.get("temp_min", 0)}, {main_data.get("temp_max", 0)},
                        {main_data.get("pressure", 0)}, {main_data.get("humidity", 0)}, {main_data.get("sea_level", 0)}, {main_data.get("grnd_level", 0)},
                        {data.get("visibility", 0)},
                        {wind.get("speed", 0)}, {wind.get("deg", 0)},
                        {clouds.get("all", 0)},
                        {data.get("dt", 0)}, {sys_data.get("sunrise", 0)}, {sys_data.get("sunset", 0)}, {data.get("timezone", 0)},
                        {sys_data.get("type", 0)}, {sys_data.get("id", 0)},
                        {data.get("cod", 0)}
                    )
                """
                cursor.execute(sql)
                
                if i % 5 == 0:
                    print(f"   Progress: {i}/{len(json_files)} records processed...")
            except Exception as e:
                print(f"✗ Error inserting record {i} ({item.get('filename', 'unknown')}): {e}")
                print(f"   Error type: {type(e).__name__}")
                raise
        
        conn.commit()
        print(f"✓ Successfully loaded {len(json_files)} record(s) into WEATHER_DATA_NORMALIZED")
        return True
    except snowflake.errors.ProgrammingError as e:
        print(f"✗ SQL execution error: {e}")
        print(f"   Error code: {e.errno if hasattr(e, 'errno') else 'N/A'}")
        print(f"   SQL state: {e.sqlstate if hasattr(e, 'sqlstate') else 'N/A'}")
        conn.rollback()
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        conn.rollback()
        return False
    finally:
        cursor.close()


def main():
    print("=" * 70)
    print("SNOWFLAKE DATA LOADER - Weather Data")
    print("=" * 70)
    
    print("\n1. Reading JSON files from weather_data directory...")
    json_files = read_json_files()
    
    if not json_files:
        print("No JSON files found!")
        print("Please ensure weather_data directory contains .json files.")
        sys.exit(1)
    
    print(f"\n✓ Found {len(json_files)} JSON file(s)")
    
    print("\n2. Connecting to Snowflake...")
    print("-" * 70)
    
    credentials = get_snowflake_credentials()
    
    if not credentials:
        print("\n⚠ Cannot proceed without Snowflake credentials.")
        print("   Please configure 'snowflake_config.json' or set environment variables.")
        sys.exit(1)
    
    conn = connect_to_snowflake(
        account=credentials['account'],
        user=credentials['user'],
        password=credentials['password'],
        warehouse=credentials['warehouse'],
        database=credentials['database'],
        schema=credentials['schema']
    )
    
    if not conn:
        print("\n⚠ Could not connect to Snowflake. Exiting.")
        sys.exit(1)
    
    try:
        print("\n" + "=" * 70)
        print("DATA LOADING")
        print("=" * 70)
        
        print("\n3. Loading data to Snowflake tables...")
        print("-" * 70)
        
        load_variant = input("Load to WEATHER_DATA_RAW table (VARIANT)? (y/n): ").strip().lower()
        if load_variant == 'y':
            load_data_to_variant_table(conn, json_files)
        
        load_normalized = input("\nLoad to WEATHER_DATA_NORMALIZED table? (y/n): ").strip().lower()
        if load_normalized == 'y':
            load_data_to_normalized_table(conn, json_files)
        
        if load_variant != 'y' and load_normalized != 'y':
            print("\n⚠ No tables selected for loading.")
        else:
            print("\n" + "=" * 70)
            print("✓ Data loading completed!")
            print("=" * 70)
    finally:
        conn.close()
        print("\n✓ Snowflake connection closed")


if __name__ == "__main__":
    main()
