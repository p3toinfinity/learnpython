#!/usr/bin/env python3
"""
Practical example: Using sys module for validation
Demonstrates real-world validation scenarios
"""

import sys
import os

def validate_python_version():
    """Validate Python version"""
    if sys.version_info < (3, 7):
        sys.stderr.write("Error: Python 3.7 or higher is required\n")
        sys.stderr.write(f"Current version: {sys.version}\n")
        sys.exit(1)
    print(f"✓ Python version OK: {sys.version_info.major}.{sys.version_info.minor}")

def validate_command_line_args():
    """Validate command-line arguments"""
    if len(sys.argv) < 2:
        sys.stderr.write("Error: Missing required argument\n")
        sys.stderr.write(f"Usage: {sys.argv[0]} <filename>\n")
        sys.exit(1)
    
    filename = sys.argv[1]
    if not os.path.exists(filename):
        sys.stderr.write(f"Error: File '{filename}' not found\n")
        sys.exit(1)
    
    print(f"✓ File exists: {filename}")
    return filename

def validate_platform():
    """Validate and show platform info"""
    print(f"✓ Platform: {sys.platform}")
    
    if sys.platform == "win32":
        print("  → Windows detected")
    elif sys.platform == "darwin":
        print("  → macOS detected")
    elif sys.platform.startswith("linux"):
        print("  → Linux detected")
    
    return sys.platform

def validate_memory_usage(data):
    """Validate data size"""
    size = sys.getsizeof(data)
    max_size = 10_000_000  # 10MB limit
    
    if size > max_size:
        sys.stderr.write(f"Error: Data too large ({size} bytes > {max_size} bytes)\n")
        sys.exit(1)
    
    print(f"✓ Data size OK: {size:,} bytes")
    return size

def validate_input_source():
    """Check if input is from terminal or pipe"""
    if sys.stdin.isatty():
        print("✓ Running interactively (terminal input)")
        return "interactive"
    else:
        print("✓ Running as script (piped input)")
        return "piped"

def validate_module_available(module_name):
    """Check if a module is available"""
    if module_name in sys.modules:
        print(f"✓ Module '{module_name}' already loaded")
        return True
    
    try:
        __import__(module_name)
        print(f"✓ Module '{module_name}' available")
        return True
    except ImportError:
        sys.stderr.write(f"Error: Module '{module_name}' not found\n")
        sys.stderr.write("Install it using: pip install {module_name}\n")
        return False

def main():
    """Main validation function"""
    print("=" * 60)
    print("VALIDATION EXAMPLE USING SYS MODULE")
    print("=" * 60)
    
    # 1. Validate Python version
    print("\n1. Validating Python version...")
    validate_python_version()
    
    # 2. Validate platform
    print("\n2. Checking platform...")
    validate_platform()
    
    # 3. Validate input source
    print("\n3. Checking input source...")
    validate_input_source()
    
    # 4. Validate required modules
    print("\n4. Checking required modules...")
    required_modules = ["json", "os"]
    all_available = True
    for module in required_modules:
        if not validate_module_available(module):
            all_available = False
    
    if not all_available:
        sys.exit(1)
    
    # 5. Validate command-line arguments (if provided)
    print("\n5. Validating command-line arguments...")
    if len(sys.argv) > 1:
        filename = validate_command_line_args()
        
        # Read and validate file size
        try:
            with open(filename, 'r') as f:
                data = f.read()
            validate_memory_usage(data)
        except Exception as e:
            sys.stderr.write(f"Error reading file: {e}\n")
            sys.exit(1)
    else:
        print("  → No command-line arguments provided (optional)")
    
    # 6. Show system info
    print("\n6. System Information:")
    print(f"  Python executable: {sys.executable}")
    print(f"  Recursion limit: {sys.getrecursionlimit()}")
    print(f"  Max integer size: {sys.maxsize}")
    print(f"  Byte order: {sys.byteorder}")
    
    print("\n" + "=" * 60)
    print("All validations passed! ✓")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.stderr.write("\n\nInterrupted by user\n")
        sys.exit(130)  # Standard exit code for Ctrl+C
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        sys.stderr.write(f"\nUnexpected error: {exc_type.__name__}: {exc_value}\n")
        sys.exit(1)

