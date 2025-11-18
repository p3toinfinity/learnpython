#!/usr/bin/env python3
"""
Different ways to exit a Python program
Demonstrates alternatives to sys.exit()
"""

import sys
import os

print("=" * 70)
print("DIFFERENT WAYS TO EXIT A PYTHON PROGRAM")
print("=" * 70)

print("\n1. sys.exit() - Recommended method")
print("-" * 70)
print("""
import sys
sys.exit(0)   # Exit with success (0)
sys.exit(1)   # Exit with error (non-zero)
sys.exit()    # Exit with code 0 (default)
""")
print("✓ Most explicit and recommended")
print("✓ Can specify exit codes")
print("✓ Raises SystemExit exception (can be caught)")

print("\n2. exit() - Built-in function")
print("-" * 70)
print("""
exit(0)   # Exit with success
exit(1)   # Exit with error
exit()    # Exit with code 0
""")
print("✓ Simple and convenient")
print("⚠ Actually calls sys.exit() internally")
print("⚠ Not recommended in production code (meant for interactive shell)")
print("Note: exit() is actually an alias for sys.exit()")

print("\n3. quit() - Built-in function")
print("-" * 70)
print("""
quit()    # Exits the program
""")
print("✓ Very simple")
print("⚠ Also calls sys.exit() internally")
print("⚠ Not recommended in production (meant for interactive shell)")
print("Note: quit() is also an alias for sys.exit()")

print("\n4. raise SystemExit() - Exception-based exit")
print("-" * 70)
print("""
raise SystemExit(0)   # Exit with success
raise SystemExit(1)   # Exit with error
raise SystemExit()    # Exit with code 0
""")
print("✓ More Pythonic (exception-based)")
print("✓ Same as sys.exit() (sys.exit() actually raises SystemExit)")
print("✓ Can be caught in try/except blocks")
print("Example:")
print("""
try:
    raise SystemExit(1)
except SystemExit as e:
    print(f"Exiting with code: {e.code}")
""")

print("\n5. os._exit() - Immediate exit (no cleanup)")
print("-" * 70)
print("""
import os
os._exit(0)   # Immediate exit, no cleanup
os._exit(1)   # Immediate exit with error code
""")
print("⚠ Bypasses all cleanup (no finally blocks, no atexit handlers)")
print("⚠ Use only in special cases (forked processes, etc.)")
print("⚠ Not recommended for normal use")
print("Example use case: Child processes after fork()")

print("\n6. Let script end naturally")
print("-" * 70)
print("""
# Just return from main function or let script reach end
def main():
    # ... do work ...
    return  # Exits with code 0

if __name__ == "__main__":
    main()
    # Script ends here naturally
""")
print("✓ Clean and Pythonic")
print("✓ Always exits with code 0")
print("✓ Good for simple scripts")

print("\n7. raise KeyboardInterrupt - For Ctrl+C")
print("-" * 70)
print("""
raise KeyboardInterrupt  # Simulates Ctrl+C
""")
print("✓ For handling user interruption")
print("✓ Standard exit code is 130")
print("Example:")
print("""
try:
    while True:
        # long running task
        pass
except KeyboardInterrupt:
    print("\\nInterrupted by user")
    sys.exit(130)  # Standard exit code for Ctrl+C
""")

print("\n" + "=" * 70)
print("COMPARISON TABLE")
print("=" * 70)
print("""
Method              | Exit Code | Cleanup | Recommended | Use Case
-------------------|-----------|---------|-------------|------------------
sys.exit(code)     | Yes       | Yes     | ✓✓✓        | Production code
exit(code)         | Yes       | Yes     | ⚠          | Interactive shell
quit()             | 0 only    | Yes     | ⚠          | Interactive shell
raise SystemExit() | Yes       | Yes     | ✓✓         | Exception-based
os._exit(code)     | Yes       | No      | ⚠⚠⚠        | Special cases only
Natural end        | 0 only    | Yes     | ✓          | Simple scripts
KeyboardInterrupt  | 130       | Yes     | ✓          | User interruption
""")

print("\n" + "=" * 70)
print("PRACTICAL EXAMPLES")
print("=" * 70)

print("\nExample 1: Using sys.exit() - Recommended")
print("-" * 70)
print("""
def validate_input(value):
    if not value:
        print("Error: Value required")
        sys.exit(1)  # Exit with error
    return value
""")

print("\nExample 2: Using raise SystemExit()")
print("-" * 70)
print("""
def process_file(filename):
    if not os.path.exists(filename):
        raise SystemExit(1)  # More Pythonic
    # ... process file ...
""")

print("\nExample 3: Catching SystemExit")
print("-" * 70)
print("""
try:
    sys.exit(1)
except SystemExit as e:
    print(f"Program exiting with code: {e.code}")
    # Can do cleanup here
    raise  # Re-raise to actually exit
""")

print("\nExample 4: Natural exit (best for simple scripts)")
print("-" * 70)
print("""
def main():
    print("Hello, World!")
    # Script ends naturally here

if __name__ == "__main__":
    main()
""")

print("\nExample 5: Handling KeyboardInterrupt")
print("-" * 70)
print("""
try:
    while True:
        data = input("Enter data (Ctrl+C to exit): ")
        process(data)
except KeyboardInterrupt:
    print("\\nExiting...")
    sys.exit(130)  # Standard exit code for Ctrl+C
""")

print("\n" + "=" * 70)
print("RECOMMENDATIONS")
print("=" * 70)
print("""
1. For production code: Use sys.exit(code)
   - Most explicit and clear
   - Can specify exit codes
   - Standard practice

2. For simple scripts: Let script end naturally
   - Clean and Pythonic
   - No need for explicit exit

3. For exception handling: raise SystemExit()
   - More Pythonic exception-based approach
   - Can be caught if needed

4. Avoid: exit() and quit()
   - Meant for interactive shell
   - Not recommended in scripts

5. Avoid: os._exit()
   - Only for special cases (forked processes)
   - Bypasses cleanup
""")

print("\n" + "=" * 70)

