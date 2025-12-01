from datetime import datetime, timedelta
import re

# Lambda to check if input is alphabetic (with possible special chars)
is_alphabetic = lambda s: bool(re.match(r'^[a-zA-Z\W]+$', s))

# Lambda to strip special chars and return uppercase
process_text = lambda s: re.sub(r'[^a-zA-Z]', '', s).upper()

# Lambda to process date
process_date = lambda date_str: (
    lambda d: (
        print(f"Original date: {d.strftime('%Y-%m-%d')}"),
        print(f"Date after adding 1 day: {(d + timedelta(days=1)).strftime('%Y-%m-%d')}")
    )
)(datetime.strptime(date_str, "%Y-%m-%d"))

# Main entry point - only runs when script is executed directly
if __name__ == "__main__":
    # Get input from console
    user_input = input("Enter a date (YYYY-MM-DD format) or text: ")
    
    # Main processing logic
    try:
        if is_alphabetic(user_input):
            result = process_text(user_input)
            print(f"Processed text: {result}")
        else:
            process_date(user_input)
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD format (e.g., 2024-01-15)")
