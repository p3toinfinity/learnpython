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


def execute_sql_in_snowflake(conn, sql_statements, table_name="table"):
    if not conn:
        print("Error: No connection to Snowflake")
        return False
    
    cursor = conn.cursor()
    try:
        statements = [
            s.strip() 
            for s in sql_statements.split(';') 
            if s.strip() and not s.strip().startswith('--')
        ]
        
        print(f"   Executing {len(statements)} INSERT statement(s) for {table_name}...")
        
        for i, statement in enumerate(statements, 1):
            if statement:
                cursor.execute(statement)
                if i % 10 == 0:
                    print(f"   Progress: {i}/{len(statements)} statements executed...")
        
        conn.commit()
        print(f"✓ Successfully loaded {len(statements)} record(s) into {table_name}")
        return True
    except snowflake.errors.ProgrammingError as e:
        print(f"✗ SQL execution error: {e}")
        print(f"   Error code: {e.errno if hasattr(e, 'errno') else 'N/A'}")
        conn.rollback()
        return False
    except Exception as e:
        print(f"✗ Unexpected error executing SQL: {e}")
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
        
        print("\n⚠ SQL generation functions have been removed.")
        print("   Please implement a data loading method (e.g., COPY INTO, bulk insert).")
    finally:
        conn.close()
        print("\n✓ Snowflake connection closed")


if __name__ == "__main__":
    main()
