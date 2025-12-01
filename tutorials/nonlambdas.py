import re

# Get input from console
user_input = input("Enter a date (YYYY-MM-DD format) or text: ")

# Function to check if input is alphabetic (with possible special chars)
def is_alphabetic(s):
    return bool(re.match(r'^[a-zA-Z\W]+$', s))

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
    if is_alphabetic(inp):
        process_text(inp)
    else:
        process_date(inp)

try:
    process(user_input)
except ValueError:
    print("Invalid date format. Please use YYYY-MM-DD format (e.g., 2024-01-15)")    