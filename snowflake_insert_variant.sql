-- Generated on: 2025-11-18 14:04:55
-- Total records: 1
-- This file contains SQL INSERT statements for Snowflake

INSERT INTO WEATHER_DATA_RAW (CITY_NAME, CITY_ID, COUNTRY_CODE, WEATHER_JSON)
VALUES ('Madurai', 1264521, 'IN', PARSE_JSON('{"coord": {"lon": 78.1167, "lat": 9.9333}, "weather": [{"id": 701, "main": "Mist", "description": "mist", "icon": "50n"}], "base": "stations", "main": {"temp": 25.01, "feels_like": 26.02, "temp_min": 25.01, "temp_max": 25.01, "pressure": 1013, "humidity": 94, "sea_level": 1013, "grnd_level": 996}, "visibility": 3000, "wind": {"speed": 1.03, "deg": 0}, "clouds": {"all": 75}, "dt": 1763485325, "sys": {"type": 1, "id": 9216, "country": "IN", "sunrise": 1763426594, "sunset": 1763468544}, "timezone": 19800, "id": 1264521, "name": "Madurai", "cod": 200}'));