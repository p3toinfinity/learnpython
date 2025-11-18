#!/usr/bin/env python3
"""
Weather Data Loader for Snowflake
==================================

This script reads weather JSON files from the weather_data directory and generates
Snowflake SQL INSERT statements for loading data into Snowflake tables.

Features:
    - Reads all JSON files from a specified directory
    - Generates SQL INSERT statements for VARIANT table (stores full JSON)
    - Generates SQL INSERT statements for Normalized table (flattened structure)
    - Saves generated SQL to files for easy execution

Requirements:
    - snowflake-connector-python (for direct connection - optional)
    - Python 3.7+

Usage:
    python load_to_snowflake.py

Output:
    - snowflake_insert_variant.sql (for WEATHER_DATA_RAW table)
    - snowflake_insert_normalized.sql (for WEATHER_DATA_NORMALIZED table)
"""

# ============================================================================
# Standard Library Imports
# ============================================================================
import json          # For parsing and formatting JSON data
import os            # For operating system interface (file operations)
import sys           # For system-specific parameters and functions
from pathlib import Path        # For modern path handling
from datetime import datetime    # For timestamp generation

# ============================================================================
# Third-Party Imports
# ============================================================================
# Snowflake connector - uncomment when ready to use direct connection
# import snowflake.connector as snowflake

def read_json_files(directory="weather_data"):
    """
    Read all JSON files from the specified directory
    
    This function scans the directory for all .json files, reads their contents,
    and returns a list of dictionaries containing the file metadata and parsed data.
    
    Args:
        directory (str): Directory path containing JSON files (default: "weather_data")
        
    Returns:
        list: List of dictionaries, each containing:
            - 'filename': Name of the JSON file
            - 'data': Parsed JSON data as Python dictionary
            - 'filepath': Full path to the file
    
    Example:
        >>> files = read_json_files("weather_data")
        >>> print(files[0]['filename'])
        'Madurai_20251118_090205.json'
    """
    # Initialize list to store all JSON file data
    json_files = []
    
    # Convert string path to Path object for easier manipulation
    weather_dir = Path(directory)
    
    # Validate that the directory exists
    if not weather_dir.exists():
        print(f"Error: Directory '{directory}' not found")
        return []
    
    # Iterate through all JSON files in the directory
    # glob("*.json") finds all files with .json extension
    for json_file in weather_dir.glob("*.json"):
        try:
            # Open file with UTF-8 encoding to handle international characters
            with open(json_file, 'r', encoding='utf-8') as f:
                # Parse JSON content into Python dictionary
                data = json.load(f)
                
                # Store file information along with parsed data
                json_files.append({
                    'filename': json_file.name,      # Just the filename (e.g., "file.json")
                    'data': data,                    # Parsed JSON data
                    'filepath': str(json_file)       # Full path to the file
                })
            print(f"✓ Read: {json_file.name}")
            
        except json.JSONDecodeError as e:
            # Handle JSON parsing errors (malformed JSON)
            print(f"✗ JSON parsing error in {json_file.name}: {e}")
        except Exception as e:
            # Handle any other file reading errors
            print(f"✗ Error reading {json_file.name}: {e}")
    
    return json_files


def generate_snowflake_insert_variant(json_data_list):
    """
    Generate Snowflake INSERT statements for VARIANT table (WEATHER_DATA_RAW)
    
    This function creates SQL INSERT statements that store the entire JSON response
    in a VARIANT column. This approach is flexible and preserves all original data,
    including nested structures and arrays.
    
    Args:
        json_data_list (list): List of dictionaries containing JSON file data
                              Each dict should have 'data' key with the JSON content
        
    Returns:
        str: Multi-line string containing all INSERT statements, separated by blank lines
        
    Note:
        - Uses PARSE_JSON() function to convert JSON string to VARIANT type
        - Escapes single quotes in JSON to prevent SQL injection
        - Extracts key fields (city_name, city_id, country_code) for easy querying
    """
    sql_statements = []
    
    # Process each JSON file's data
    for item in json_data_list:
        # Extract the parsed JSON data
        data = item['data']
        
        # Extract key fields for separate columns (for easier querying)
        # Using .get() with defaults to handle missing fields gracefully
        city_name = data.get('name', 'Unknown')
        city_id = data.get('id', 0)
        country_code = data.get('sys', {}).get('country', '')  # Nested access with default
        
        # Convert Python dictionary back to JSON string
        # This ensures proper JSON formatting for Snowflake
        json_str = json.dumps(data)
        
        # Escape single quotes in JSON string to prevent SQL syntax errors
        # Snowflake uses single quotes for string literals, so we need to escape them
        json_str = json_str.replace("'", "''")
        
        # Build the INSERT statement
        # PARSE_JSON() converts the JSON string into Snowflake's VARIANT data type
        sql = f"""INSERT INTO WEATHER_DATA_RAW (CITY_NAME, CITY_ID, COUNTRY_CODE, WEATHER_JSON)
VALUES ('{city_name}', {city_id}, '{country_code}', PARSE_JSON('{json_str}'));"""
        
        sql_statements.append(sql)
    
    # Join all INSERT statements with double newlines for readability
    return '\n\n'.join(sql_statements)


def generate_snowflake_insert_normalized(json_data_list):
    """
    Generate Snowflake INSERT statements for Normalized table (WEATHER_DATA_NORMALIZED)
    
    This function creates SQL INSERT statements that flatten the nested JSON structure
    into individual columns. This approach provides better query performance and
    easier data access, but requires the schema to match the JSON structure.
    
    Args:
        json_data_list (list): List of dictionaries containing JSON file data
                              Each dict should have 'data' key with the JSON content
        
    Returns:
        str: Multi-line string containing all INSERT statements, separated by blank lines
        
    Note:
        - Flattens nested JSON objects (coord, main, wind, clouds, sys)
        - Extracts first element from weather array (typically only one weather condition)
        - Handles missing fields with default values
        - Escapes single quotes in string values to prevent SQL errors
    """
    sql_statements = []
    
    # Process each JSON file's data
    for item in json_data_list:
        data = item['data']
        
        # ====================================================================
        # Extract Nested Data Structures
        # ====================================================================
        # Coordinates: longitude and latitude
        coord = data.get('coord', {})
        
        # Weather array: typically contains one object, but we handle empty arrays
        weather_array = data.get('weather', [{}])
        weather = weather_array[0] if weather_array else {}  # Get first element or empty dict
        
        # Main weather data: temperature, pressure, humidity, etc.
        main_data = data.get('main', {})
        
        # Wind information: speed and direction
        wind = data.get('wind', {})
        
        # Cloud coverage percentage
        clouds = data.get('clouds', {})
        
        # System information: country, sunrise, sunset, etc.
        sys_data = data.get('sys', {})
        
        # ====================================================================
        # Build INSERT Statement
        # ====================================================================
        # Create a formatted INSERT statement with all columns
        # Using .get() with defaults ensures missing fields don't cause errors
        sql = f"""INSERT INTO WEATHER_DATA_NORMALIZED (
    -- City Information
    CITY_NAME, CITY_ID, COUNTRY_CODE,
    -- Geographic Coordinates
    LONGITUDE, LATITUDE,
    -- Weather Conditions (from weather array)
    WEATHER_ID, WEATHER_MAIN, WEATHER_DESCRIPTION, WEATHER_ICON,
    -- Base Information
    BASE,
    -- Temperature Data
    TEMPERATURE, FEELS_LIKE, TEMP_MIN, TEMP_MAX,
    -- Atmospheric Data
    PRESSURE, HUMIDITY, SEA_LEVEL, GROUND_LEVEL,
    -- Visibility
    VISIBILITY,
    -- Wind Data
    WIND_SPEED, WIND_DEGREE,
    -- Cloud Data
    CLOUD_COVERAGE,
    -- Timestamps (Unix timestamps)
    DATA_TIMESTAMP, SUNRISE_TIMESTAMP, SUNSET_TIMESTAMP, TIMEZONE_OFFSET,
    -- System Information
    SYS_TYPE, SYS_ID,
    -- Response Code
    RESPONSE_CODE
) VALUES (
    -- City Information
    '{data.get("name", "")}',
    {data.get("id", 0)},
    '{sys_data.get("country", "")}',
    -- Geographic Coordinates
    {coord.get("lon", 0)},
    {coord.get("lat", 0)},
    -- Weather Conditions
    {weather.get("id", 0)},
    '{weather.get("main", "")}',
    '{weather.get("description", "").replace("'", "''")}',  -- Escape single quotes
    '{weather.get("icon", "")}',
    -- Base
    '{data.get("base", "")}',
    -- Temperature Values
    {main_data.get("temp", 0)},
    {main_data.get("feels_like", 0)},
    {main_data.get("temp_min", 0)},
    {main_data.get("temp_max", 0)},
    -- Atmospheric Values
    {main_data.get("pressure", 0)},
    {main_data.get("humidity", 0)},
    {main_data.get("sea_level", 0)},
    {main_data.get("grnd_level", 0)},
    -- Visibility
    {data.get("visibility", 0)},
    -- Wind Values
    {wind.get("speed", 0)},
    {wind.get("deg", 0)},
    -- Cloud Coverage
    {clouds.get("all", 0)},
    -- Unix Timestamps
    {data.get("dt", 0)},              -- Data timestamp
    {sys_data.get("sunrise", 0)},     -- Sunrise timestamp
    {sys_data.get("sunset", 0)},       -- Sunset timestamp
    {data.get("timezone", 0)},        -- Timezone offset in seconds
    -- System Information
    {sys_data.get("type", 0)},
    {sys_data.get("id", 0)},
    -- Response Code
    {data.get("cod", 0)}
);"""
        
        sql_statements.append(sql)
    
    # Join all INSERT statements with double newlines for readability
    return '\n\n'.join(sql_statements)


def save_sql_to_file(sql_statements, filename="snowflake_insert_statements.sql"):
    """
    Save SQL INSERT statements to a file with metadata header
    
    This function writes the generated SQL statements to a file, adding
    a header comment with generation timestamp and record count.
    
    Args:
        sql_statements (str): Multi-line string containing SQL INSERT statements
        filename (str): Output filename for the SQL file (default: "snowflake_insert_statements.sql")
    
    Note:
        - File is written with UTF-8 encoding to support international characters
        - Adds header comments with generation metadata
        - Counts INSERT statements to show total records
    """
    # Open file in write mode with UTF-8 encoding
    with open(filename, 'w', encoding='utf-8') as f:
        # Write header comments with metadata
        f.write(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- Total records: {sql_statements.count('INSERT')}\n")
        f.write(f"-- This file contains SQL INSERT statements for Snowflake\n\n")
        
        # Write the actual SQL statements
        f.write(sql_statements)
    
    print(f"\n✓ SQL statements saved to: {filename}")


def main():
    """
    Main execution function
    
    Orchestrates the entire process:
    1. Reads JSON files from weather_data directory
    2. Generates SQL INSERT statements for both table types
    3. Saves SQL statements to files
    4. Provides next steps for the user
    """
    # ========================================================================
    # Print Welcome Banner
    # ========================================================================
    print("=" * 70)
    print("SNOWFLAKE DATA LOADER - Weather Data")
    print("=" * 70)
    
    # ========================================================================
    # Step 1: Read JSON Files
    # ========================================================================
    print("\n1. Reading JSON files from weather_data directory...")
    json_files = read_json_files()
    
    # Validate that we found at least one JSON file
    if not json_files:
        print("No JSON files found!")
        print("Please ensure weather_data directory contains .json files.")
        sys.exit(1)
    
    print(f"\n✓ Found {len(json_files)} JSON file(s)")
    
    # ========================================================================
    # Step 2: Generate SQL INSERT Statements
    # ========================================================================
    print("\n2. Generating SQL INSERT statements...")
    
    # ------------------------------------------------------------------------
    # Option 1: VARIANT Table (WEATHER_DATA_RAW)
    # ------------------------------------------------------------------------
    # This table stores the entire JSON in a VARIANT column
    # Best for: Flexibility, preserving all data, easy schema changes
    print("\n   Generating for VARIANT table (WEATHER_DATA_RAW)...")
    variant_sql = generate_snowflake_insert_variant(json_files)
    save_sql_to_file(variant_sql, "snowflake_insert_variant.sql")
    
    # ------------------------------------------------------------------------
    # Option 2: Normalized Table (WEATHER_DATA_NORMALIZED)
    # ------------------------------------------------------------------------
    # This table flattens the JSON into individual columns
    # Best for: Query performance, easy data access, structured queries
    print("\n   Generating for Normalized table (WEATHER_DATA_NORMALIZED)...")
    normalized_sql = generate_snowflake_insert_normalized(json_files)
    save_sql_to_file(normalized_sql, "snowflake_insert_normalized.sql")
    
    # ========================================================================
    # Step 3: Completion Message and Next Steps
    # ========================================================================
    print("\n" + "=" * 70)
    print("✓ SQL files generated successfully!")
    print("=" * 70)
    print("\nNext steps:")
    print("1. Review the generated SQL files")
    print("   - snowflake_insert_variant.sql (for VARIANT table)")
    print("   - snowflake_insert_normalized.sql (for normalized table)")
    print("2. Run the DDL script (snowflake_weather_ddl.sql) in Snowflake")
    print("   - This creates the tables if they don't exist")
    print("3. Execute the INSERT statements in Snowflake")
    print("   - You can run the generated SQL files directly in Snowflake")
    print("\nAlternative: Use Snowflake's COPY INTO command with a stage")
    print("             for bulk loading large datasets.")


# ============================================================================
# Script Entry Point
# ============================================================================
if __name__ == "__main__":
    # Execute main function when script is run directly
    main()

