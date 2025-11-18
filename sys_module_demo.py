#!/usr/bin/env python3
"""
Comprehensive demo of sys module functions useful for validation and code control
"""

import sys
import os

print("=" * 70)
print("SYS MODULE - USEFUL FUNCTIONS FOR VALIDATION & CODE CONTROL")
print("=" * 70)

# 1. sys.exit() - Exit the program
print("\n1. sys.exit() - Exit program with status code")
print("-" * 70)
print("Usage: sys.exit(0) for success, sys.exit(1) for error")
print("Example: if not api_key: sys.exit(1)")

# 2. sys.argv - Command line arguments
print("\n2. sys.argv - Command line arguments validation")
print("-" * 70)
print(f"sys.argv = {sys.argv}")
print(f"Number of arguments: {len(sys.argv)}")
print("Useful for validating command-line inputs:")
print("""
if len(sys.argv) < 2:
    print("Error: Missing required argument")
    sys.exit(1)

filename = sys.argv[1]
if not os.path.exists(filename):
    print(f"Error: File '{filename}' not found")
    sys.exit(1)
""")

# 3. sys.stdin, sys.stdout, sys.stderr - Standard streams
print("\n3. sys.stdin/stdout/stderr - Input/Output streams")
print("-" * 70)
print("sys.stdin  - Standard input (keyboard)")
print("sys.stdout - Standard output (console)")
print("sys.stderr - Standard error (error messages)")
print("""
# Redirect error messages
sys.stderr.write("This is an error message\\n")

# Read from stdin
data = sys.stdin.read()  # Reads all input until EOF
""")

# 4. sys.platform - Platform detection
print("\n4. sys.platform - Platform/OS validation")
print("-" * 70)
print(f"Current platform: {sys.platform}")
print("Useful for OS-specific code:")
print("""
if sys.platform == "win32":
    # Windows-specific code
    pass
elif sys.platform == "darwin":
    # macOS-specific code
    pass
elif sys.platform.startswith("linux"):
    # Linux-specific code
    pass
""")

# 5. sys.version - Python version validation
print("\n5. sys.version & sys.version_info - Python version check")
print("-" * 70)
print(f"Python version string: {sys.version}")
print(f"Python version info: {sys.version_info}")
print(f"Major version: {sys.version_info.major}")
print(f"Minor version: {sys.version_info.minor}")
print("Useful for version validation:")
print("""
if sys.version_info < (3, 7):
    print("Error: Python 3.7 or higher required")
    sys.exit(1)

# Or check specific version
if sys.version_info.major != 3:
    print("Error: Python 3 required")
    sys.exit(1)
""")

# 6. sys.path - Module search path
print("\n6. sys.path - Module/import path validation")
print("-" * 70)
print(f"Number of paths: {len(sys.path)}")
print("First few paths:")
for i, path in enumerate(sys.path[:3]):
    print(f"  {i+1}. {path}")
print("Useful for checking if module can be imported:")
print("""
import sys
if '/custom/path' not in sys.path:
    sys.path.append('/custom/path')
""")

# 7. sys.modules - Check if module is loaded
print("\n7. sys.modules - Check loaded modules")
print("-" * 70)
print(f"Number of loaded modules: {len(sys.modules)}")
print("Check if module is already imported:")
print("""
if 'requests' in sys.modules:
    print("requests module is already loaded")
else:
    print("requests module not loaded yet")
""")

# 8. sys.getsizeof() - Object size validation
print("\n8. sys.getsizeof() - Object size check")
print("-" * 70)
data_small = "hello"
data_large = "x" * 1000000
print(f"Size of '{data_small}': {sys.getsizeof(data_small)} bytes")
print(f"Size of large string: {sys.getsizeof(data_large)} bytes")
print("Useful for memory validation:")
print("""
data = get_large_data()
if sys.getsizeof(data) > 100000000:  # 100MB
    print("Warning: Data too large!")
    sys.exit(1)
""")

# 9. sys.maxsize - Maximum integer size
print("\n9. sys.maxsize - Maximum integer validation")
print("-" * 70)
print(f"Maximum integer size: {sys.maxsize}")
print("Useful for integer range validation:")
print("""
user_input = int(input("Enter number: "))
if user_input > sys.maxsize:
    print("Error: Number too large")
    sys.exit(1)
""")

# 10. sys.exc_info() - Exception information
print("\n10. sys.exc_info() - Get exception details")
print("-" * 70)
print("Useful for error handling and validation:")
print("""
try:
    risky_operation()
except Exception:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    print(f"Exception type: {exc_type.__name__}")
    print(f"Exception message: {exc_value}")
    # Log or handle the exception
""")

# 11. sys.getrecursionlimit() - Recursion depth
print("\n11. sys.getrecursionlimit() - Recursion depth check")
print("-" * 70)
print(f"Current recursion limit: {sys.getrecursionlimit()}")
print("Useful for preventing stack overflow:")
print("""
def recursive_function(depth):
    if depth > sys.getrecursionlimit() - 100:
        print("Warning: Approaching recursion limit")
        return
    # ... rest of function
""")

# 12. sys.flags - Python interpreter flags
print("\n12. sys.flags - Interpreter flags")
print("-" * 70)
print("Useful for checking Python runtime options:")
print(f"Debug mode: {sys.flags.debug}")
print(f"Optimize mode: {sys.flags.optimize}")

# 13. sys.stdin.isatty() - Check if interactive terminal
print("\n13. sys.stdin.isatty() - Check if running interactively")
print("-" * 70)
print(f"Is interactive terminal: {sys.stdin.isatty()}")
print("Useful for conditional input:")
print("""
if sys.stdin.isatty():
    # Running in terminal - can use input()
    user_input = input("Enter value: ")
else:
    # Running as script - read from stdin
    user_input = sys.stdin.read().strip()
""")

# 14. sys.executable - Python interpreter path
print("\n14. sys.executable - Python interpreter path")
print("-" * 70)
print(f"Python executable: {sys.executable}")
print("Useful for subprocess calls:")

# 15. sys.byteorder - Byte order
print("\n15. sys.byteorder - System byte order")
print("-" * 70)
print(f"Byte order: {sys.byteorder}")
print("Useful for binary data handling")

print("\n" + "=" * 70)
print("PRACTICAL VALIDATION EXAMPLES")
print("=" * 70)

print("""
# Example 1: Validate command-line arguments
if len(sys.argv) < 2:
    print("Usage: python script.py <filename>")
    sys.exit(1)

# Example 2: Validate Python version
if sys.version_info < (3, 7):
    print("Error: Requires Python 3.7+")
    sys.exit(1)

# Example 3: Validate file exists (using sys.argv)
import os
filename = sys.argv[1]
if not os.path.exists(filename):
    sys.stderr.write(f"Error: File '{filename}' not found\\n")
    sys.exit(1)

# Example 4: Validate input size
data = sys.stdin.read()
if sys.getsizeof(data) > 10_000_000:  # 10MB limit
    print("Error: Input too large")
    sys.exit(1)

# Example 5: Platform-specific validation
if sys.platform == "win32":
    # Windows-specific validation
    pass
else:
    # Unix/Linux/macOS validation
    pass
""")

print("\n" + "=" * 70)

