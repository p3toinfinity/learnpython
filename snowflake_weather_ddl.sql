-- ============================================================================
-- Snowflake DDL Script for Weather Data
-- Based on OpenWeatherMap API JSON Response
-- ============================================================================

-- Option 1: Table with VARIANT column (stores entire JSON)
-- Best for: Flexible storage, easy to add new fields, simple ingestion
-- ============================================================================

CREATE OR REPLACE TABLE WEATHER_DATA_RAW (
    RECORD_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    CITY_NAME VARCHAR(100),
    CITY_ID NUMBER,
    COUNTRY_CODE VARCHAR(10),
    RECORD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    WEATHER_JSON VARIANT,
    LOAD_DATE DATE DEFAULT CURRENT_DATE()
);

-- Add comments for documentation
COMMENT ON TABLE WEATHER_DATA_RAW IS 'Raw weather data from OpenWeatherMap API stored as JSON VARIANT';
COMMENT ON COLUMN WEATHER_DATA_RAW.WEATHER_JSON IS 'Complete JSON response from OpenWeatherMap API';
COMMENT ON COLUMN WEATHER_DATA_RAW.RECORD_TIMESTAMP IS 'Timestamp when record was inserted into Snowflake';

-- ============================================================================
-- Option 2: Normalized Table Structure (fully flattened)
-- Best for: Easy querying, better performance for specific fields
-- ============================================================================

CREATE OR REPLACE TABLE WEATHER_DATA_NORMALIZED (
    -- Primary Key
    RECORD_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    
    -- City Information
    CITY_NAME VARCHAR(100),
    CITY_ID NUMBER,
    COUNTRY_CODE VARCHAR(10),
    
    -- Coordinates
    LONGITUDE FLOAT,
    LATITUDE FLOAT,
    
    -- Weather Conditions (from weather array - taking first element)
    WEATHER_ID NUMBER,
    WEATHER_MAIN VARCHAR(50),
    WEATHER_DESCRIPTION VARCHAR(100),
    WEATHER_ICON VARCHAR(10),
    
    -- Base
    BASE VARCHAR(50),
    
    -- Main Weather Data
    TEMPERATURE FLOAT,
    FEELS_LIKE FLOAT,
    TEMP_MIN FLOAT,
    TEMP_MAX FLOAT,
    PRESSURE NUMBER,
    HUMIDITY NUMBER,
    SEA_LEVEL NUMBER,
    GROUND_LEVEL NUMBER,
    
    -- Visibility
    VISIBILITY NUMBER,
    
    -- Wind
    WIND_SPEED FLOAT,
    WIND_DEGREE NUMBER,
    
    -- Clouds
    CLOUD_COVERAGE NUMBER,
    
    -- Timestamps
    DATA_TIMESTAMP NUMBER,  -- Unix timestamp from API
    SUNRISE_TIMESTAMP NUMBER,  -- Unix timestamp
    SUNSET_TIMESTAMP NUMBER,   -- Unix timestamp
    TIMEZONE_OFFSET NUMBER,    -- Timezone offset in seconds
    
    -- System Info
    SYS_TYPE NUMBER,
    SYS_ID NUMBER,
    
    -- Response Code
    RESPONSE_CODE NUMBER,
    
    -- Metadata
    RECORD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LOAD_DATE DATE DEFAULT CURRENT_DATE(),
    
    -- Primary Key Constraint
    PRIMARY KEY (RECORD_ID)
);

-- Add comments for documentation
COMMENT ON TABLE WEATHER_DATA_NORMALIZED IS 'Normalized weather data from OpenWeatherMap API';
COMMENT ON COLUMN WEATHER_DATA_NORMALIZED.DATA_TIMESTAMP IS 'Unix timestamp from API (dt field)';
COMMENT ON COLUMN WEATHER_DATA_NORMALIZED.SUNRISE_TIMESTAMP IS 'Unix timestamp for sunrise';
COMMENT ON COLUMN WEATHER_DATA_NORMALIZED.SUNSET_TIMESTAMP IS 'Unix timestamp for sunset';
COMMENT ON COLUMN WEATHER_DATA_NORMALIZED.TIMEZONE_OFFSET IS 'Timezone offset in seconds (e.g., 19800 for IST = UTC+5:30)';

-- ============================================================================
-- Option 3: Hybrid Approach (VARIANT + Key Columns)
-- Best for: Balance between flexibility and queryability
-- ============================================================================

CREATE OR REPLACE TABLE WEATHER_DATA_HYBRID (
    RECORD_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    
    -- Key columns for easy querying
    CITY_NAME VARCHAR(100),
    CITY_ID NUMBER,
    COUNTRY_CODE VARCHAR(10),
    TEMPERATURE FLOAT,
    HUMIDITY NUMBER,
    WEATHER_DESCRIPTION VARCHAR(100),
    DATA_TIMESTAMP NUMBER,  -- Unix timestamp from API
    
    -- Full JSON for flexibility
    WEATHER_JSON VARIANT,
    
    -- Metadata
    RECORD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LOAD_DATE DATE DEFAULT CURRENT_DATE()
);

COMMENT ON TABLE WEATHER_DATA_HYBRID IS 'Hybrid weather data table with key columns and full JSON';

-- ============================================================================
-- Create Views for Easy Querying (if using VARIANT table)
-- ============================================================================

-- View to extract key fields from VARIANT column
CREATE OR REPLACE VIEW WEATHER_DATA_VIEW AS
SELECT
    RECORD_ID,
    CITY_NAME,
    CITY_ID,
    COUNTRY_CODE,
    RECORD_TIMESTAMP,
    LOAD_DATE,
    -- Extract from JSON
    WEATHER_JSON:coord.lon::FLOAT AS LONGITUDE,
    WEATHER_JSON:coord.lat::FLOAT AS LATITUDE,
    WEATHER_JSON:weather[0].id::NUMBER AS WEATHER_ID,
    WEATHER_JSON:weather[0].main::VARCHAR AS WEATHER_MAIN,
    WEATHER_JSON:weather[0].description::VARCHAR AS WEATHER_DESCRIPTION,
    WEATHER_JSON:weather[0].icon::VARCHAR AS WEATHER_ICON,
    WEATHER_JSON:base::VARCHAR AS BASE,
    WEATHER_JSON:main.temp::FLOAT AS TEMPERATURE,
    WEATHER_JSON:main.feels_like::FLOAT AS FEELS_LIKE,
    WEATHER_JSON:main.temp_min::FLOAT AS TEMP_MIN,
    WEATHER_JSON:main.temp_max::FLOAT AS TEMP_MAX,
    WEATHER_JSON:main.pressure::NUMBER AS PRESSURE,
    WEATHER_JSON:main.humidity::NUMBER AS HUMIDITY,
    WEATHER_JSON:main.sea_level::NUMBER AS SEA_LEVEL,
    WEATHER_JSON:main.grnd_level::NUMBER AS GROUND_LEVEL,
    WEATHER_JSON:visibility::NUMBER AS VISIBILITY,
    WEATHER_JSON:wind.speed::FLOAT AS WIND_SPEED,
    WEATHER_JSON:wind.deg::NUMBER AS WIND_DEGREE,
    WEATHER_JSON:clouds.all::NUMBER AS CLOUD_COVERAGE,
    WEATHER_JSON:dt::NUMBER AS DATA_TIMESTAMP,
    WEATHER_JSON:sys.type::NUMBER AS SYS_TYPE,
    WEATHER_JSON:sys.id::NUMBER AS SYS_ID,
    WEATHER_JSON:sys.country::VARCHAR AS COUNTRY_CODE_FROM_SYS,
    WEATHER_JSON:sys.sunrise::NUMBER AS SUNRISE_TIMESTAMP,
    WEATHER_JSON:sys.sunset::NUMBER AS SUNSET_TIMESTAMP,
    WEATHER_JSON:timezone::NUMBER AS TIMEZONE_OFFSET,
    WEATHER_JSON:id::NUMBER AS CITY_ID_FROM_JSON,
    WEATHER_JSON:name::VARCHAR AS CITY_NAME_FROM_JSON,
    WEATHER_JSON:cod::NUMBER AS RESPONSE_CODE,
    -- Convert Unix timestamps to readable format
    TO_TIMESTAMP_NTZ(WEATHER_JSON:dt::NUMBER) AS DATA_DATETIME,
    TO_TIMESTAMP_NTZ(WEATHER_JSON:sys.sunrise::NUMBER) AS SUNRISE_DATETIME,
    TO_TIMESTAMP_NTZ(WEATHER_JSON:sys.sunset::NUMBER) AS SUNSET_DATETIME
FROM WEATHER_DATA_RAW;

COMMENT ON VIEW WEATHER_DATA_VIEW IS 'View to extract and flatten weather data from VARIANT JSON column';

-- ============================================================================
-- Sample INSERT Statements
-- ============================================================================

-- Insert into VARIANT table (Option 1)
/*
INSERT INTO WEATHER_DATA_RAW (CITY_NAME, CITY_ID, COUNTRY_CODE, WEATHER_JSON)
SELECT 
    'Madurai' AS CITY_NAME,
    1264521 AS CITY_ID,
    'IN' AS COUNTRY_CODE,
    PARSE_JSON('{
        "coord": {"lon": 78.1167, "lat": 9.9333},
        "weather": [{"id": 701, "main": "Mist", "description": "mist", "icon": "50n"}],
        "base": "stations",
        "main": {"temp": 25.01, "feels_like": 26.02, "temp_min": 25.01, "temp_max": 25.01, 
                 "pressure": 1013, "humidity": 94, "sea_level": 1013, "grnd_level": 996},
        "visibility": 3000,
        "wind": {"speed": 1.03, "deg": 0},
        "clouds": {"all": 75},
        "dt": 1763485325,
        "sys": {"type": 1, "id": 9216, "country": "IN", "sunrise": 1763426594, "sunset": 1763468544},
        "timezone": 19800,
        "id": 1264521,
        "name": "Madurai",
        "cod": 200
    }') AS WEATHER_JSON;
*/

-- Insert into Normalized table (Option 2)
/*
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
)
VALUES (
    'Madurai', 1264521, 'IN',
    78.1167, 9.9333,
    701, 'Mist', 'mist', '50n',
    'stations',
    25.01, 26.02, 25.01, 25.01,
    1013, 94, 1013, 996,
    3000,
    1.03, 0,
    75,
    1763485325, 1763426594, 1763468544, 19800,
    1, 9216,
    200
);
*/

-- ============================================================================
-- Useful Queries
-- ============================================================================

-- Query from VARIANT table using JSON path
/*
SELECT 
    CITY_NAME,
    WEATHER_JSON:main.temp::FLOAT AS TEMPERATURE,
    WEATHER_JSON:main.humidity::NUMBER AS HUMIDITY,
    WEATHER_JSON:weather[0].description::VARCHAR AS CONDITION
FROM WEATHER_DATA_RAW
WHERE CITY_NAME = 'Madurai';
*/

-- Query from Normalized table (simpler)
/*
SELECT 
    CITY_NAME,
    TEMPERATURE,
    HUMIDITY,
    WEATHER_DESCRIPTION AS CONDITION
FROM WEATHER_DATA_NORMALIZED
WHERE CITY_NAME = 'Madurai';
*/

-- Query with timestamp conversion
/*
SELECT 
    CITY_NAME,
    TEMPERATURE,
    TO_TIMESTAMP_NTZ(DATA_TIMESTAMP) AS WEATHER_TIME,
    TO_TIMESTAMP_NTZ(SUNRISE_TIMESTAMP) AS SUNRISE_TIME,
    TO_TIMESTAMP_NTZ(SUNSET_TIMESTAMP) AS SUNSET_TIME
FROM WEATHER_DATA_NORMALIZED
ORDER BY DATA_TIMESTAMP DESC;
*/

