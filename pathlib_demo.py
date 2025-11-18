#!/usr/bin/env python3
"""
pathlib Module Demonstration
=============================

pathlib is a modern Python module (introduced in Python 3.4) that provides
an object-oriented approach to working with filesystem paths.

Key Benefits:
    - Cross-platform path handling (Windows, macOS, Linux)
    - More intuitive than os.path
    - Object-oriented API
    - Automatic path normalization
"""

from pathlib import Path
import os

print("=" * 70)
print("PATHLIB MODULE EXPLANATION")
print("=" * 70)

# ============================================================================
# 1. WHAT IS PATHLIB?
# ============================================================================
print("\n1. WHAT IS PATHLIB?")
print("-" * 70)
print("""
pathlib provides the Path class, which represents filesystem paths as objects
instead of strings. This makes path manipulation more intuitive and safer.

Old way (os.path):
    import os
    file_path = os.path.join("folder", "subfolder", "file.txt")
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            data = f.read()

New way (pathlib):
    from pathlib import Path
    file_path = Path("folder") / "subfolder" / "file.txt"
    if file_path.exists():
        data = file_path.read_text()
""")

# ============================================================================
# 2. CREATING PATH OBJECTS
# ============================================================================
print("\n2. CREATING PATH OBJECTS")
print("-" * 70)

# Create a Path object
current_dir = Path(".")
weather_dir = Path("weather_data")
absolute_path = Path("/Users/karthikdhina/PycharmProjects/learnpython")

print(f"Current directory Path: {current_dir}")
print(f"Weather data Path: {weather_dir}")
print(f"Absolute path: {absolute_path}")

# ============================================================================
# 3. PATH OPERATIONS
# ============================================================================
print("\n3. PATH OPERATIONS")
print("-" * 70)

# Joining paths using / operator (works on all platforms!)
path1 = Path("folder") / "subfolder" / "file.txt"
print(f"Joined path: {path1}")

# Multiple ways to join
path2 = Path("folder").joinpath("subfolder", "file.txt")
print(f"Using joinpath(): {path2}")

# Parent directory
file_path = Path("weather_data/file.json")
print(f"File path: {file_path}")
print(f"Parent directory: {file_path.parent}")
print(f"Parent's parent: {file_path.parent.parent}")

# ============================================================================
# 4. PATH PROPERTIES
# ============================================================================
print("\n4. PATH PROPERTIES")
print("-" * 70)

example_path = Path("weather_data/Madurai_20251118_090205.json")

print(f"Full path: {example_path}")
print(f"Name (filename): {example_path.name}")
print(f"Stem (filename without extension): {example_path.stem}")
print(f"Suffix (extension): {example_path.suffix}")
print(f"Parent: {example_path.parent}")
print(f"Parts: {list(example_path.parts)}")

# ============================================================================
# 5. CHECKING PATH EXISTENCE AND TYPE
# ============================================================================
print("\n5. CHECKING PATH EXISTENCE AND TYPE")
print("-" * 70)

# Check if path exists
weather_path = Path("weather_data")
print(f"Does 'weather_data' exist? {weather_path.exists()}")

# Check if it's a file or directory
if weather_path.exists():
    print(f"Is directory? {weather_path.is_dir()}")
    print(f"Is file? {weather_path.is_file()}")

# Check if file exists
json_file = Path("weather_data/Madurai_20251118_090205.json")
if json_file.exists():
    print(f"\nFile exists: {json_file}")
    print(f"File size: {json_file.stat().st_size} bytes")

# ============================================================================
# 6. FINDING FILES (GLOB PATTERNS)
# ============================================================================
print("\n6. FINDING FILES (GLOB PATTERNS)")
print("-" * 70)

# Find all JSON files in weather_data directory
weather_dir = Path("weather_data")
if weather_dir.exists():
    json_files = list(weather_dir.glob("*.json"))
    print(f"Found {len(json_files)} JSON file(s):")
    for file in json_files:
        print(f"  - {file.name}")

# Recursive glob (search in subdirectories)
# all_json = list(Path(".").glob("**/*.json"))

# ============================================================================
# 7. READING AND WRITING FILES
# ============================================================================
print("\n7. READING AND WRITING FILES")
print("-" * 70)

# Read text file
json_file = Path("weather_data/Madurai_20251118_090205.json")
if json_file.exists():
    print(f"Reading: {json_file.name}")
    # Path.read_text() reads entire file as string
    content = json_file.read_text(encoding='utf-8')
    print(f"  File size: {len(content)} characters")
    
    # Path.read_bytes() reads as bytes
    # bytes_content = json_file.read_bytes()

# Write to file
test_file = Path("test_output.txt")
test_file.write_text("Hello from pathlib!", encoding='utf-8')
print(f"\nCreated test file: {test_file}")
print(f"Content: {test_file.read_text()}")

# Clean up
test_file.unlink()  # Delete the file
print(f"Deleted test file")

# ============================================================================
# 8. CREATING DIRECTORIES
# ============================================================================
print("\n8. CREATING DIRECTORIES")
print("-" * 70)

# Create single directory
new_dir = Path("test_directory")
if not new_dir.exists():
    new_dir.mkdir()
    print(f"Created directory: {new_dir}")

# Create nested directories (parents=True creates parent dirs if needed)
nested_dir = Path("level1/level2/level3")
nested_dir.mkdir(parents=True, exist_ok=True)
print(f"Created nested directories: {nested_dir}")

# Clean up
import shutil
if new_dir.exists():
    shutil.rmtree(new_dir)
if Path("level1").exists():
    shutil.rmtree("level1")
print("Cleaned up test directories")

# ============================================================================
# 9. HOW IT'S USED IN YOUR CODE
# ============================================================================
print("\n9. HOW IT'S USED IN YOUR load_to_snowflake.py")
print("-" * 70)
print("""
In your code, pathlib is used like this:

    from pathlib import Path
    
    weather_dir = Path(directory)  # Convert string to Path object
    
    if not weather_dir.exists():   # Check if directory exists
        return []
    
    for json_file in weather_dir.glob("*.json"):  # Find all .json files
        # json_file is a Path object
        with open(json_file, 'r') as f:  # Can use Path object directly
            data = json.load(f)
        
        json_files.append({
            'filename': json_file.name,      # Get just the filename
            'filepath': str(json_file)       # Convert to string if needed
        })

Benefits in your code:
    ✓ weather_dir.glob("*.json") - Easy file pattern matching
    ✓ json_file.name - Get filename without path manipulation
    ✓ Cross-platform - Works on Windows, macOS, Linux
    ✓ More readable than os.path.join()
""")

# ============================================================================
# 10. COMPARISON: os.path vs pathlib
# ============================================================================
print("\n10. COMPARISON: os.path vs pathlib")
print("-" * 70)

# Old way with os.path
print("Old way (os.path):")
old_path = os.path.join("folder", "subfolder", "file.txt")
print(f"  Path: {old_path}")
print(f"  Exists: {os.path.exists(old_path)}")
print(f"  Basename: {os.path.basename(old_path)}")

# New way with pathlib
print("\nNew way (pathlib):")
new_path = Path("folder") / "subfolder" / "file.txt"
print(f"  Path: {new_path}")
print(f"  Exists: {new_path.exists()}")
print(f"  Name: {new_path.name}")

print("\n" + "=" * 70)
print("KEY TAKEAWAYS")
print("=" * 70)
print("""
✓ pathlib.Path represents paths as objects, not strings
✓ Use / operator to join paths (works on all platforms)
✓ Methods like .exists(), .is_file(), .is_dir() are intuitive
✓ .glob() makes file searching easy
✓ .read_text() and .write_text() simplify file I/O
✓ More Pythonic and modern than os.path
✓ Automatically handles platform differences (Windows vs Unix)
""")

