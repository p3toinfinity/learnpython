#!/usr/bin/env python3
"""
Optimized MySQL Data Loader using Pandas, NumPy, and SQLAlchemy
Designed for large-scale data loading (100,000+ records)
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm
import boto3
from botocore.exceptions import ClientError, NoCredentialsError


class WeatherDataLoader:
    """Optimized weather data loader using pandas and numpy for large datasets."""
    
    # Batch sizes for processing
    JSON_BATCH_SIZE = 10000  # Process JSON files in batches
    MYSQL_BATCH_SIZE = 5000  # Insert records in batches to MySQL
    
    def __init__(self, config_path: str = "mysql_config.json", aws_config_path: str = "aws_config.json"):
        """Initialize the loader with configuration."""
        self.config_path = config_path
        self.aws_config_path = aws_config_path
        self.engine = None
        self.aws_config = None
        self.s3_client = None
        
    def load_aws_config(self) -> Optional[Dict]:
        """
        Loads AWS configuration from a JSON file
        
        Returns:
            dict: AWS configuration dictionary, None if failed
        """
        config_file = Path(self.aws_config_path)
        
        if not config_file.exists():
            print(f"✗ Error: AWS config file '{self.aws_config_path}' not found.")
            return None
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            aws_config = config.get('aws', {})
            required_fields = ['access_key_id', 'secret_access_key', 'bucket_name']
            
            if not all(field in aws_config and aws_config[field] for field in required_fields):
                print(f"✗ Error: Missing required fields in AWS config. Required: {required_fields}")
                return None
            
            print(f"✓ Loaded AWS configuration from: {self.aws_config_path}")
            return aws_config
            
        except json.JSONDecodeError as e:
            print(f"✗ Error: Invalid JSON in AWS config file: {e}")
            return None
        except Exception as e:
            print(f"✗ Error loading AWS config: {e}")
            return None
    
    def initialize_s3_client(self, aws_config: Dict):
        """
        Initialize S3 client with AWS credentials
        
        Args:
            aws_config: AWS configuration dictionary
        """
        try:
            region = aws_config.get('region', 'us-east-1')
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_config['access_key_id'],
                aws_secret_access_key=aws_config['secret_access_key'],
                region_name=region
            )
            print(f"✓ Initialized S3 client for region: {region}")
        except Exception as e:
            print(f"✗ Error initializing S3 client: {e}")
            raise
    
    def list_s3_json_files(self, aws_config: Dict) -> List[Dict]:
        """
        List all JSON files in S3 bucket
        
        Args:
            aws_config: AWS configuration dictionary
            
        Returns:
            List of dictionaries with 'Key' (S3 object key) and 'Size' (file size)
        """
        bucket_name = aws_config['bucket_name']
        s3_prefix = aws_config.get('s3_prefix', '').strip()
        
        json_files = []
        continuation_token = None
        
        try:
            while True:
                list_kwargs = {
                    'Bucket': bucket_name,
                    'Prefix': s3_prefix,
                }
                
                if continuation_token:
                    list_kwargs['ContinuationToken'] = continuation_token
                
                response = self.s3_client.list_objects_v2(**list_kwargs)
                
                if 'Contents' not in response:
                    break
                
                # Filter for JSON files only
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.lower().endswith('.json'):
                        json_files.append({
                            'Key': key,
                            'Size': obj['Size'],
                            'LastModified': obj.get('LastModified')
                        })
                
                # Check if there are more objects
                if not response.get('IsTruncated', False):
                    break
                
                continuation_token = response.get('NextContinuationToken')
            
            return json_files
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == 'NoSuchBucket':
                print(f"✗ Error: S3 bucket '{bucket_name}' does not exist.")
            elif error_code == 'AccessDenied':
                print(f"✗ Error: Access denied to bucket '{bucket_name}'. Check your AWS permissions.")
            else:
                print(f"✗ Error listing S3 objects: {e}")
            return []
        except Exception as e:
            print(f"✗ Error listing S3 files: {e}")
            return []
    
    def read_json_from_s3(self, s3_key: str, aws_config: Dict) -> Optional[Dict]:
        """
        Read and parse a JSON file from S3
        
        Args:
            s3_key: S3 object key (path)
            aws_config: AWS configuration dictionary
            
        Returns:
            Parsed JSON data as dictionary, None if failed
        """
        bucket_name = aws_config['bucket_name']
        
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except json.JSONDecodeError as e:
            print(f"✗ JSON parsing error in S3 object '{s3_key}': {e}")
            return None
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == 'NoSuchKey':
                print(f"✗ Error: S3 object '{s3_key}' not found.")
            else:
                print(f"✗ Error reading S3 object '{s3_key}': {e}")
            return None
        except Exception as e:
            print(f"✗ Error reading S3 object '{s3_key}': {e}")
            return None
    
    def read_json_files_pandas(self, aws_config: Optional[Dict] = None) -> pd.DataFrame:
        """
        Read all JSON files from S3 using pandas for efficient processing.
        Returns a DataFrame with file metadata and raw JSON data.
        
        Args:
            aws_config: Optional AWS config (if None, will load from file)
        """
        # Load AWS config if not provided
        if aws_config is None:
            aws_config = self.load_aws_config()
            if not aws_config:
                return pd.DataFrame()
        
        self.aws_config = aws_config
        
        # Initialize S3 client
        try:
            self.initialize_s3_client(aws_config)
        except Exception:
            return pd.DataFrame()
        
        # List all JSON files in S3
        print(f"\nListing JSON files from S3 bucket...")
        json_files = self.list_s3_json_files(aws_config)
        
        if not json_files:
            bucket_name = aws_config['bucket_name']
            s3_prefix = aws_config.get('s3_prefix', '')
            print(f"✗ No JSON files found in s3://{bucket_name}/{s3_prefix}")
            return pd.DataFrame()
        
        print(f"✓ Found {len(json_files)} JSON file(s) in S3")
        print(f"  Processing in batches of {self.JSON_BATCH_SIZE}...")
        
        # Process files in batches to manage memory
        all_dataframes = []
        
        for i in range(0, len(json_files), self.JSON_BATCH_SIZE):
            batch = json_files[i:i + self.JSON_BATCH_SIZE]
            batch_data = []
            
            for s3_file in tqdm(batch, desc=f"Reading batch {i//self.JSON_BATCH_SIZE + 1}", unit="files"):
                s3_key = s3_file['Key']
                try:
                    # Read JSON from S3
                    data = self.read_json_from_s3(s3_key, aws_config)
                    
                    if data is None:
                        continue
                    
                    # Normalize nested JSON structure using pandas
                    normalized = pd.json_normalize(data)
                    # Add metadata
                    normalized['filename'] = Path(s3_key).name  # Extract filename from S3 key
                    normalized['filepath'] = s3_key  # Full S3 path
                    normalized['raw_json'] = json.dumps(data)  # Store as string for JSON column
                    
                    batch_data.append(normalized)
                except Exception as e:
                    print(f"\n✗ Error processing S3 object '{s3_key}': {e}")
            
            if batch_data:
                # Concatenate all DataFrames in batch
                batch_df = pd.concat(batch_data, ignore_index=True)
                all_dataframes.append(batch_df)
                print(f"  ✓ Processed batch {i//self.JSON_BATCH_SIZE + 1}: {len(batch_df)} files")
        
        if not all_dataframes:
            return pd.DataFrame()
        
        # Concatenate all batches into single DataFrame
        df = pd.concat(all_dataframes, ignore_index=True)
        print(f"\n✓ Total records loaded: {len(df)}")
        print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        return df
    
    def normalize_dataframe_for_raw_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for WEATHER_DATA_RAW table (JSON column).
        Extracts key fields and keeps full JSON.
        """
        if df.empty:
            return pd.DataFrame()
        
        result_df = pd.DataFrame()
        
        # Extract key fields
        result_df['CITY_NAME'] = df.get('name', pd.Series(['Unknown'] * len(df))).fillna('Unknown')
        result_df['CITY_ID'] = pd.to_numeric(df.get('id', 0), errors='coerce').fillna(0).astype(np.int64)
        result_df['COUNTRY_CODE'] = df.get('sys.country', pd.Series([''] * len(df))).fillna('')
        
        # Keep raw JSON string
        if 'raw_json' in df.columns:
            result_df['WEATHER_JSON'] = df['raw_json']
        else:
            # Reconstruct JSON if needed
            result_df['WEATHER_JSON'] = df.apply(
                lambda row: json.dumps({
                    'coord': row.get('coord', {}),
                    'weather': row.get('weather', []),
                    'base': row.get('base', ''),
                    'main': row.get('main', {}),
                    'visibility': row.get('visibility', 0),
                    'wind': row.get('wind', {}),
                    'clouds': row.get('clouds', {}),
                    'dt': row.get('dt', 0),
                    'sys': row.get('sys', {}),
                    'timezone': row.get('timezone', 0),
                    'id': row.get('id', 0),
                    'name': row.get('name', ''),
                    'cod': row.get('cod', 0)
                }), axis=1
            )
        
        return result_df
    
    def normalize_dataframe_for_normalized_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for WEATHER_DATA_NORMALIZED table.
        Fully flattens all nested JSON structures.
        """
        if df.empty:
            return pd.DataFrame()
        
        result_df = pd.DataFrame()
        
        # City Information
        result_df['CITY_NAME'] = df.get('name', pd.Series([''] * len(df))).fillna('')
        result_df['CITY_ID'] = pd.to_numeric(df.get('id', 0), errors='coerce').fillna(0).astype(np.int64)
        result_df['COUNTRY_CODE'] = df.get('sys.country', pd.Series([''] * len(df))).fillna('')
        
        # Coordinates - using pandas operations for efficiency
        result_df['LONGITUDE'] = pd.to_numeric(
            df.get('coord.lon', 0), errors='coerce'
        ).fillna(0).astype(np.float64)
        result_df['LATITUDE'] = pd.to_numeric(
            df.get('coord.lat', 0), errors='coerce'
        ).fillna(0).astype(np.float64)
        
        # Weather (taking first element from array)
        # Try normalized format first (weather[0].id), then fall back to extracting from list
        if 'weather[0].id' in df.columns:
            # Already normalized by json_normalize
            result_df['WEATHER_ID'] = pd.to_numeric(df.get('weather[0].id', 0), errors='coerce').fillna(0).astype(np.int64)
            result_df['WEATHER_MAIN'] = df.get('weather[0].main', pd.Series([''] * len(df))).fillna('')
            result_df['WEATHER_DESCRIPTION'] = df.get('weather[0].description', pd.Series([''] * len(df))).fillna('')
            result_df['WEATHER_ICON'] = df.get('weather[0].icon', pd.Series([''] * len(df))).fillna('')
        else:
            # Handle nested weather array - extract first element
            def extract_weather_field(field):
                def extract(row):
                    weather = row.get('weather', [])
                    if isinstance(weather, list) and len(weather) > 0:
                        if isinstance(weather[0], dict):
                            return weather[0].get(field, 0 if field == 'id' else '')
                        return weather[0] if field == 'id' else ''
                    return 0 if field == 'id' else ''
                return df.apply(extract, axis=1)
            
            result_df['WEATHER_ID'] = pd.to_numeric(extract_weather_field('id'), errors='coerce').fillna(0).astype(np.int64)
            result_df['WEATHER_MAIN'] = extract_weather_field('main').fillna('')
            result_df['WEATHER_DESCRIPTION'] = extract_weather_field('description').fillna('')
            result_df['WEATHER_ICON'] = extract_weather_field('icon').fillna('')
        
        # Base
        result_df['BASE'] = df.get('base', pd.Series([''] * len(df))).fillna('')
        
        # Main weather data - using pandas vectorized operations
        result_df['TEMPERATURE'] = pd.to_numeric(df.get('main.temp', 0), errors='coerce').fillna(0).astype(np.float64)
        result_df['FEELS_LIKE'] = pd.to_numeric(df.get('main.feels_like', 0), errors='coerce').fillna(0).astype(np.float64)
        result_df['TEMP_MIN'] = pd.to_numeric(df.get('main.temp_min', 0), errors='coerce').fillna(0).astype(np.float64)
        result_df['TEMP_MAX'] = pd.to_numeric(df.get('main.temp_max', 0), errors='coerce').fillna(0).astype(np.float64)
        result_df['PRESSURE'] = pd.to_numeric(df.get('main.pressure', 0), errors='coerce').fillna(0).astype(np.int64)
        result_df['HUMIDITY'] = pd.to_numeric(df.get('main.humidity', 0), errors='coerce').fillna(0).astype(np.int64)
        result_df['SEA_LEVEL'] = pd.to_numeric(df.get('main.sea_level', 0), errors='coerce').fillna(0).astype(np.int64)
        result_df['GROUND_LEVEL'] = pd.to_numeric(df.get('main.grnd_level', 0), errors='coerce').fillna(0).astype(np.int64)
        
        # Visibility
        result_df['VISIBILITY'] = pd.to_numeric(df.get('visibility', 0), errors='coerce').fillna(0).astype(np.int64)
        
        # Wind
        result_df['WIND_SPEED'] = pd.to_numeric(df.get('wind.speed', 0), errors='coerce').fillna(0).astype(np.float64)
        result_df['WIND_DEGREE'] = pd.to_numeric(df.get('wind.deg', 0), errors='coerce').fillna(0).astype(np.int64)
        
        # Clouds
        result_df['CLOUD_COVERAGE'] = pd.to_numeric(df.get('clouds.all', 0), errors='coerce').fillna(0).astype(np.int64)
        
        # Timestamps
        result_df['DATA_TIMESTAMP'] = pd.to_numeric(df.get('dt', 0), errors='coerce').fillna(0).astype(np.int64)
        result_df['SUNRISE_TIMESTAMP'] = pd.to_numeric(df.get('sys.sunrise', 0), errors='coerce').fillna(0).astype(np.int64)
        result_df['SUNSET_TIMESTAMP'] = pd.to_numeric(df.get('sys.sunset', 0), errors='coerce').fillna(0).astype(np.int64)
        result_df['TIMEZONE_OFFSET'] = pd.to_numeric(df.get('timezone', 0), errors='coerce').fillna(0).astype(np.int64)
        
        # System Info
        result_df['SYS_TYPE'] = pd.to_numeric(df.get('sys.type', 0), errors='coerce').fillna(0).astype(np.int64)
        result_df['SYS_ID'] = pd.to_numeric(df.get('sys.id', 0), errors='coerce').fillna(0).astype(np.int64)
        
        # Response Code
        result_df['RESPONSE_CODE'] = pd.to_numeric(df.get('cod', 0), errors='coerce').fillna(0).astype(np.int64)
        
        # Replace NaN with None for SQL compatibility
        result_df = result_df.replace({np.nan: None})
        
        return result_df
    
    def load_config_file(self) -> Optional[Dict]:
        """Load MySQL configuration from file."""
        config_file = Path(self.config_path)
        
        if not config_file.exists():
            return None
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            mysql_config = config.get('mysql', {})
            required_fields = ['host', 'user', 'password', 'database']
            
            if all(field in mysql_config and mysql_config[field] for field in required_fields):
                print(f"\n✓ Found MySQL credentials in config file: {self.config_path}")
                return {
                    'host': mysql_config['host'],
                    'port': mysql_config.get('port', 3306),
                    'user': mysql_config['user'],
                    'password': mysql_config['password'],
                    'database': mysql_config['database'],
                    'charset': mysql_config.get('charset', 'utf8mb4')
                }
            else:
                missing_fields = [field for field in required_fields if not mysql_config.get(field)]
                print(f"\n⚠ Config file found but missing required fields: {', '.join(missing_fields)}")
                return None
        except json.JSONDecodeError as e:
            print(f"\n✗ Error parsing config file {self.config_path}: {e}")
            return None
        except Exception as e:
            print(f"\n✗ Error reading config file {self.config_path}: {e}")
            return None
    
    def get_mysql_credentials(self) -> Optional[Dict]:
        """Get MySQL credentials from config file or environment variables."""
        config_creds = self.load_config_file()
        if config_creds:
            return config_creds
        
        host = os.getenv("MYSQL_HOST")
        port = os.getenv("MYSQL_PORT", "3306")
        user = os.getenv("MYSQL_USER")
        password = os.getenv("MYSQL_PASSWORD")
        database = os.getenv("MYSQL_DATABASE")
        charset = os.getenv("MYSQL_CHARSET", "utf8mb4")
        
        if all([host, user, password, database]):
            print("\n✓ Found MySQL credentials in environment variables")
            return {
                'host': host,
                'port': int(port),
                'user': user,
                'password': password,
                'database': database,
                'charset': charset
            }
        
        print("\n⚠ No MySQL credentials found.")
        print("   Please configure 'mysql_config.json' or set environment variables.")
        return None
    
    def connect_to_mysql(self, credentials: Dict) -> bool:
        """Connect to MySQL using SQLAlchemy."""
        try:
            # Build connection string
            connection_string = (
                f"mysql+pymysql://{credentials['user']}:{credentials['password']}"
                f"@{credentials['host']}:{credentials['port']}/{credentials['database']}"
                f"?charset={credentials['charset']}"
            )
            
            print(f"\nConnecting to MySQL at {credentials['host']}:{credentials['port']}...")
            
            # Create engine with connection pooling
            self.engine = create_engine(
                connection_string,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,  # Verify connections before using
                echo=False  # Set to True for SQL query logging
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            print("✓ Connected to MySQL successfully!")
            return True
            
        except SQLAlchemyError as e:
            print(f"\n✗ Error connecting to MySQL: {e}")
            return False
        except Exception as e:
            print(f"\n✗ Unexpected error connecting to MySQL: {e}")
            return False
    
    def load_dataframe_to_raw_table(self, df: pd.DataFrame) -> bool:
        """
        Load DataFrame to WEATHER_DATA_RAW table using SQLAlchemy.
        Optimized for large datasets (100k+ records).
        """
        if df.empty:
            print("⚠ No data to load.")
            return False
        
        try:
            print(f"\nLoading {len(df):,} record(s) into WEATHER_DATA_RAW table...")
            
            # Check existing count
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM WEATHER_DATA_RAW"))
                existing_count = result.scalar()
                print(f"   Current records in table: {existing_count:,}")
            
            # Use pandas to_sql with method='multi' for batch inserts
            # Process in chunks to manage memory
            total_records = len(df)
            chunks_processed = 0
            
            with tqdm(total=total_records, desc="Loading to WEATHER_DATA_RAW", unit="records") as pbar:
                for i in range(0, total_records, self.MYSQL_BATCH_SIZE):
                    chunk_df = df.iloc[i:i + self.MYSQL_BATCH_SIZE]
                    
                    # Use to_sql with if_exists='append' and method='multi' for batch inserts
                    chunk_df.to_sql(
                        name='WEATHER_DATA_RAW',
                        con=self.engine,
                        if_exists='append',
                        index=False,
                        method='multi',  # Multi-row INSERT for better performance
                        chunksize=1000  # Internal chunking for very large batches
                    )
                    
                    chunks_processed += len(chunk_df)
                    pbar.update(len(chunk_df))
            
            print(f"✓ Successfully loaded {len(df):,} record(s) into WEATHER_DATA_RAW")
            return True
            
        except SQLAlchemyError as e:
            print(f"✗ Error loading data: {e}")
            import traceback
            traceback.print_exc()
            return False
        except Exception as e:
            print(f"✗ Error loading data: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def load_dataframe_to_normalized_table(self, df: pd.DataFrame) -> bool:
        """
        Load DataFrame to WEATHER_DATA_NORMALIZED table using SQLAlchemy.
        Optimized for large datasets (100k+ records).
        """
        if df.empty:
            print("⚠ No data to load.")
            return False
        
        try:
            print(f"\nLoading {len(df):,} record(s) into WEATHER_DATA_NORMALIZED table...")
            
            # Use pandas to_sql with method='multi' for batch inserts
            # Process in chunks to manage memory
            total_records = len(df)
            
            with tqdm(total=total_records, desc="Loading to WEATHER_DATA_NORMALIZED", unit="records") as pbar:
                for i in range(0, total_records, self.MYSQL_BATCH_SIZE):
                    chunk_df = df.iloc[i:i + self.MYSQL_BATCH_SIZE]
                    
                    # Use to_sql with if_exists='append' and method='multi' for batch inserts
                    chunk_df.to_sql(
                        name='WEATHER_DATA_NORMALIZED',
                        con=self.engine,
                        if_exists='append',
                        index=False,
                        method='multi',  # Multi-row INSERT for better performance
                        chunksize=1000  # Internal chunking for very large batches
                    )
                    
                    pbar.update(len(chunk_df))
            
            print(f"✓ Successfully loaded {len(df):,} record(s) into WEATHER_DATA_NORMALIZED")
            return True
            
        except SQLAlchemyError as e:
            print(f"✗ Error loading data: {e}")
            import traceback
            traceback.print_exc()
            return False
        except Exception as e:
            print(f"✗ Error loading data: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def close_connection(self):
        """Close MySQL connection."""
        if self.engine:
            self.engine.dispose()
            print("\n✓ MySQL connection closed")
    
    def run(self, load_raw: bool = True, load_normalized: bool = True):
        """
        Main execution method.
        
        Args:
            load_raw: Whether to load to RAW table
            load_normalized: Whether to load to NORMALIZED table
        """
        print("=" * 70)
        print("MYSQL DATA LOADER - Optimized (Pandas/NumPy/SQLAlchemy)")
        print("Loading from AWS S3")
        print("=" * 70)
        
        # Step 1: Load AWS config and read JSON files from S3
        print("\n[Step 1] Reading JSON files from S3 using Pandas...")
        df = self.read_json_files_pandas()
        
        if df.empty:
            print("✗ No data to process. Exiting.")
            return
        
        # Step 2: Connect to MySQL
        print("\n[Step 2] Connecting to MySQL...")
        credentials = self.get_mysql_credentials()
        
        if not credentials:
            print("✗ Cannot proceed without credentials. Exiting.")
            return
        
        if not self.connect_to_mysql(credentials):
            print("✗ Could not connect to MySQL. Exiting.")
            return
        
        try:
            # Step 3: Process and load data
            print("\n[Step 3] Processing and loading data...")
            print("-" * 70)
            
            success = False
            
            if load_raw:
                df_raw = self.normalize_dataframe_for_raw_table(df)
                if not df_raw.empty:
                    success = self.load_dataframe_to_raw_table(df_raw) or success
            
            if load_normalized:
                df_normalized = self.normalize_dataframe_for_normalized_table(df)
                if not df_normalized.empty:
                    success = self.load_dataframe_to_normalized_table(df_normalized) or success
            
            if success:
                print("\n" + "=" * 70)
                print("✓ Data loading completed successfully!")
                print("=" * 70)
            else:
                print("\n⚠ No data was loaded.")
                
        finally:
            self.close_connection()


def main():
    """Main entry point."""
    loader = WeatherDataLoader()
    
    # Interactive mode
    load_raw = input("Load to WEATHER_DATA_RAW table (JSON)? (y/n): ").strip().lower() == 'y'
    load_normalized = input("Load to WEATHER_DATA_NORMALIZED table? (y/n): ").strip().lower() == 'y'
    
    if not load_raw and not load_normalized:
        print("⚠ No tables selected for loading.")
        return
    
    loader.run(load_raw=load_raw, load_normalized=load_normalized)


if __name__ == "__main__":
    main()

