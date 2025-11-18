-- Generated on: 2025-11-18 14:04:55
-- Total records: 1
-- This file contains SQL INSERT statements for Snowflake

INSERT INTO WEATHER_DATA_NORMALIZED (
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
    'Madurai',
    1264521,
    'IN',
    -- Geographic Coordinates
    78.1167,
    9.9333,
    -- Weather Conditions
    701,
    'Mist',
    'mist',  -- Escape single quotes
    '50n',
    -- Base
    'stations',
    -- Temperature Values
    25.01,
    26.02,
    25.01,
    25.01,
    -- Atmospheric Values
    1013,
    94,
    1013,
    996,
    -- Visibility
    3000,
    -- Wind Values
    1.03,
    0,
    -- Cloud Coverage
    75,
    -- Unix Timestamps
    1763485325,              -- Data timestamp
    1763426594,     -- Sunrise timestamp
    1763468544,       -- Sunset timestamp
    19800,        -- Timezone offset in seconds
    -- System Information
    1,
    9216,
    -- Response Code
    200
);