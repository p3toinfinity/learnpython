import re
from datetime import datetime, timedelta


# Function to check if input is alphabetic (with possible special chars)
def is_alphabetic(s):
    return bool(re.match(r'^[a-zA-Z\W]+$', s))

# Function to check if input starts with a digit and contains no alphabets
def is_numeric_input(s):
    # Check if it starts with digit and has no alphabets
    return bool(re.match(r'^\d', s)) and not bool(re.search(r'[a-zA-Z]', s))

# Function to process integer (remove commas and convert)
def process_integer(s):
    # Remove commas and convert to int
    cleaned = s.replace(',', '')
    num = int(cleaned)
    print(f"Input: {s}")
    print(f"Converted to integer: {num}")
    print(f"Integer value: {num:,}")  # Display with comma formatting

# Function to process float
def process_float(s):
    # Remove commas (thousand separators) and convert to float
    cleaned = s.replace(',', '')
    num = float(cleaned)
    print(f"Input: {s}")
    print(f"Converted to float: {num}")
    print(f"Float value: {num:,.2f}")  # Display with 2 decimal places

# Function to strip special chars and return uppercase
def process_text(s):
    result = re.sub(r'[^a-zA-Z]', '', s).upper()
    print(f"Processed text: {result}")

# Function to process date
def process_date(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    print(f"Original date: {date_obj.strftime('%Y-%m-%d')}")
    next_day = date_obj + timedelta(days=1)
    print(f"Date after adding 1 day: {next_day.strftime('%Y-%m-%d')}")

# Main processing logic
def process(inp):
    # First check if it's a numeric input (starts with digit, no alphabets)
    if is_numeric_input(inp):
        # Check if it contains a decimal point followed by digits (float)
        if re.search(r'\.\d+', inp):
            try:
                process_float(inp)
            except ValueError:
                print(f"Invalid float format: {inp}")
        else:
            # It's an integer (may have commas)
            try:
                process_integer(inp)
            except ValueError:
                print(f"Invalid integer format: {inp}")
    elif is_alphabetic(inp):
        process_text(inp)
    else:
        # Try to process as date
        try:
            process_date(inp)
        except ValueError:
            print(f"Invalid input format: {inp}")

# Main entry point - only runs when script is executed directly
if __name__ == "__main__":
    try:
        # Get input from console
        user_input = input("Enter a date (YYYY-MM-DD format), number, or text: ")
        process(user_input)
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")    