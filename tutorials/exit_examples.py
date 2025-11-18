#!/usr/bin/env python3
"""
Practical examples of different exit methods
Run each function to see different exit behaviors
"""

import sys
import os

def example_sys_exit():
    """Example using sys.exit() - RECOMMENDED"""
    print("Using sys.exit(0) - will exit with success code")
    print("(Commented out so script continues)")
    # sys.exit(0)  # Uncomment to actually exit

def example_exit():
    """Example using exit() - NOT RECOMMENDED"""
    print("Using exit() - same as sys.exit() but not recommended")
    print("(Commented out so script continues)")
    # exit(0)  # Uncomment to actually exit

def example_quit():
    """Example using quit() - NOT RECOMMENDED"""
    print("Using quit() - same as sys.exit() but not recommended")
    print("(Commented out so script continues)")
    # quit()  # Uncomment to actually exit

def example_raise_systemexit():
    """Example using raise SystemExit()"""
    print("Using raise SystemExit(0) - exception-based exit")
    print("(Commented out so script continues)")
    # raise SystemExit(0)  # Uncomment to actually exit

def example_natural_exit():
    """Example of natural exit - just return"""
    print("Natural exit - function just returns")
    return  # Script continues after this

def example_catch_systemexit():
    """Example of catching SystemExit"""
    print("\nDemonstrating catching SystemExit:")
    try:
        print("  About to call sys.exit(42)...")
        sys.exit(42)
    except SystemExit as e:
        print(f"  Caught SystemExit with code: {e.code}")
        print("  (Re-raising to actually exit)")
        raise  # Re-raise to actually exit

def example_keyboard_interrupt():
    """Example handling KeyboardInterrupt"""
    print("\nTo test KeyboardInterrupt:")
    print("  - Uncomment the code and run")
    print("  - Press Ctrl+C to interrupt")
    # try:
    #     while True:
    #         user_input = input("Press Ctrl+C to exit: ")
    # except KeyboardInterrupt:
    #     print("\nInterrupted by user!")
    #     sys.exit(130)

def example_os_exit():
    """Example using os._exit() - DANGEROUS, NOT RECOMMENDED"""
    print("\n⚠ os._exit() - Bypasses all cleanup!")
    print("  (Commented out - would exit immediately without cleanup)")
    # os._exit(0)  # DON'T USE unless you know what you're doing

def demonstrate_exit_codes():
    """Show how exit codes work"""
    print("\n" + "=" * 60)
    print("EXIT CODES DEMONSTRATION")
    print("=" * 60)
    print("""
Exit codes convention:
  0  = Success
  1  = General error
  2  = Misuse of shell command
  130 = Script terminated by Ctrl+C
  Any other non-zero = Error

You can check exit code after running a script:
  $ python script.py
  $ echo $?  # Shows exit code (0 = success, non-zero = error)
    """)

def main():
    """Main function demonstrating all exit methods"""
    print("=" * 60)
    print("EXIT METHODS PRACTICAL EXAMPLES")
    print("=" * 60)
    
    print("\n1. sys.exit() - Recommended")
    example_sys_exit()
    
    print("\n2. exit() - Not recommended")
    example_exit()
    
    print("\n3. quit() - Not recommended")
    example_quit()
    
    print("\n4. raise SystemExit() - Alternative")
    example_raise_systemexit()
    
    print("\n5. Natural exit - Just return")
    example_natural_exit()
    
    print("\n6. Catching SystemExit")
    print("(This will actually exit, so commented out)")
    # example_catch_systemexit()
    
    print("\n7. KeyboardInterrupt handling")
    example_keyboard_interrupt()
    
    print("\n8. os._exit() - Dangerous")
    example_os_exit()
    
    demonstrate_exit_codes()
    
    print("\n" + "=" * 60)
    print("Script ending naturally (exit code 0)")
    print("=" * 60)
    # No explicit exit needed - script ends here naturally

if __name__ == "__main__":
    main()

