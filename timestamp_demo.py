#!/usr/bin/env python3
"""
Demo script to explain Unix timestamps and local time conversion
"""

from datetime import datetime
import time

print("=" * 60)
print("UNIX TIMESTAMP EXPLANATION")
print("=" * 60)

# Example from your weather data
sunrise_timestamp = 1763426348
sunset_timestamp = 1763467752
timezone_offset = 19800  # seconds (IST = UTC+5:30)

print("\n1. WHAT IS A UNIX TIMESTAMP?")
print("-" * 60)
print("A Unix timestamp is the number of seconds that have elapsed")
print("since January 1, 1970, 00:00:00 UTC (Coordinated Universal Time).")
print("It's also called 'Epoch time' or 'POSIX time'.")
print(f"\nExample: {sunrise_timestamp} seconds since Jan 1, 1970 00:00:00 UTC")

print("\n2. WHY USE UNIX TIMESTAMPS?")
print("-" * 60)
print("✓ Universal format - same number everywhere in the world")
print("✓ Easy to calculate differences (just subtract)")
print("✓ No timezone confusion - always in UTC")
print("✓ Compact storage - just one number")

print("\n3. CONVERTING TO READABLE TIME")
print("-" * 60)

# Convert to UTC datetime
sunrise_utc = datetime.utcfromtimestamp(sunrise_timestamp)
sunset_utc = datetime.utcfromtimestamp(sunset_timestamp)

print(f"Sunrise timestamp: {sunrise_timestamp}")
print(f"  → UTC time:      {sunrise_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"  → Local (IST):   {(sunrise_utc.timestamp() + timezone_offset):.0f} seconds")
print(f"    (IST = UTC + 5:30 = UTC + {timezone_offset} seconds)")

# Convert to local time (IST)
sunrise_local = datetime.fromtimestamp(sunrise_timestamp + timezone_offset)
sunset_local = datetime.fromtimestamp(sunset_timestamp + timezone_offset)

print(f"\nSunset timestamp:  {sunset_timestamp}")
print(f"  → UTC time:      {sunset_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"  → Local (IST):   {sunset_local.strftime('%Y-%m-%d %H:%M:%S')} IST")

print("\n4. HOW THE API WORKS")
print("-" * 60)
print("The OpenWeatherMap API returns:")
print("  - sunrise/sunset: Unix timestamps (in UTC)")
print("  - timezone: Offset in seconds from UTC")
print("  - For Chennai: timezone = 19800 seconds = UTC+5:30 (IST)")
print("\nTo get local time, you can:")
print("  Option 1: Add timezone offset to timestamp")
print("  Option 2: Use datetime.fromtimestamp() which uses system timezone")

print("\n5. PRACTICAL EXAMPLE")
print("-" * 60)

# Get current timestamp
current_timestamp = int(time.time())
current_utc = datetime.utcfromtimestamp(current_timestamp)
current_local = datetime.fromtimestamp(current_timestamp)

print(f"Current Unix timestamp: {current_timestamp}")
print(f"Current UTC time:        {current_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"Current local time:      {current_local.strftime('%Y-%m-%d %H:%M:%S')}")

print("\n6. CONVERSION FUNCTIONS")
print("-" * 60)
print("""
# Convert Unix timestamp to readable date/time
from datetime import datetime

timestamp = 1763426348
dt = datetime.fromtimestamp(timestamp)  # Uses system timezone
print(dt.strftime('%Y-%m-%d %H:%M:%S'))

# Convert date/time to Unix timestamp
dt = datetime(2025, 11, 17, 6, 9, 8)
timestamp = int(dt.timestamp())
print(timestamp)
""")

print("\n7. YOUR WEATHER DATA EXAMPLE")
print("-" * 60)
print("From your JSON:")
print(f"  sunrise: {sunrise_timestamp}")
print(f"  sunset:  {sunset_timestamp}")
print(f"  timezone: {timezone_offset} seconds (IST)")

# The API actually returns local sunrise/sunset times as UTC timestamps
# So we need to interpret them correctly
sunrise_readable = datetime.fromtimestamp(sunrise_timestamp)
sunset_readable = datetime.fromtimestamp(sunset_timestamp)

print(f"\nConverted to readable format:")
print(f"  Sunrise: {sunrise_readable.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Sunset:  {sunset_readable.strftime('%Y-%m-%d %H:%M:%S')}")

print("\n" + "=" * 60)

