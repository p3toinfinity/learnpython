"""
Tutorial: Mutable vs Immutable Types in Python

Mutable objects can be changed after creation.
Immutable objects cannot be changed after creation.
"""

print("=" * 60)
print("IMMUTABLE TYPES")
print("=" * 60)

# ========== IMMUTABLE: Integers ==========
print("\n1. INTEGERS (Immutable):")
x = 10
print(f"x = {x}, id(x) = {id(x)}")
x = 20  # This creates a NEW object, doesn't modify the old one
print(f"x = {x}, id(x) = {id(x)} (new object!)")

# ========== IMMUTABLE: Strings ==========
print("\n2. STRINGS (Immutable):")
s = "Hello"
print(f"s = '{s}', id(s) = {id(s)}")
s = s + " World"  # Creates a NEW string object
print(f"s = '{s}', id(s) = {id(s)} (new object!)")

# Trying to modify a string directly (this will fail)
try:
    s[0] = 'h'  # This will raise an error
except TypeError as e:
    print(f"Error: {e}")

# ========== IMMUTABLE: Tuples ==========
print("\n3. TUPLES (Immutable):")
t = (1, 2, 3)
print(f"t = {t}, id(t) = {id(t)}")
# t[0] = 10  # This would raise TypeError: 'tuple' object does not support item assignment
t = (10, 2, 3)  # Creates a NEW tuple
print(f"t = {t}, id(t) = {id(t)} (new object!)")

# Tuples can contain mutable objects
print("\n4. TUPLES WITH MUTABLE ELEMENTS:")
nested = ([1, 2], [3, 4])
print(f"nested = {nested}, id(nested) = {id(nested)}")
nested[0].append(5)  # This works! The list inside is mutable
print(f"nested = {nested}, id(nested) = {id(nested)} (same object, inner list changed)")

# ========== IMMUTABLE: Frozenset ==========
print("\n5. FROZENSET (Immutable):")
fs = frozenset([1, 2, 3])
print(f"fs = {fs}, id(fs) = {id(fs)}")
# fs.add(4)  # This would raise AttributeError
fs = frozenset([1, 2, 3, 4])  # Creates a NEW frozenset
print(f"fs = {fs}, id(fs) = {id(fs)} (new object!)")

print("\n" + "=" * 60)
print("MUTABLE TYPES")
print("=" * 60)

# ========== MUTABLE: Lists ==========
print("\n1. LISTS (Mutable):")
my_list = [1, 2, 3]
print(f"my_list = {my_list}, id(my_list) = {id(my_list)}")
my_list.append(4)  # Modifies the SAME object
print(f"my_list = {my_list}, id(my_list) = {id(my_list)} (same object!)")
my_list[0] = 10  # Modifies in place
print(f"my_list = {my_list}, id(my_list) = {id(my_list)} (same object!)")

# ========== MUTABLE: Dictionaries ==========
print("\n2. DICTIONARIES (Mutable):")
my_dict = {"name": "Alice", "age": 30}
print(f"my_dict = {my_dict}, id(my_dict) = {id(my_dict)}")
my_dict["age"] = 31  # Modifies the SAME object
print(f"my_dict = {my_dict}, id(my_dict) = {id(my_dict)} (same object!)")
my_dict["city"] = "NYC"  # Adds new key-value pair
print(f"my_dict = {my_dict}, id(my_dict) = {id(my_dict)} (same object!)")

# ========== MUTABLE: Sets ==========
print("\n3. SETS (Mutable):")
my_set = {1, 2, 3}
print(f"my_set = {my_set}, id(my_set) = {id(my_set)}")
my_set.add(4)  # Modifies the SAME object
print(f"my_set = {my_set}, id(my_set) = {id(my_set)} (same object!)")
my_set.remove(1)  # Modifies in place
print(f"my_set = {my_set}, id(my_set) = {id(my_set)} (same object!)")

print("\n" + "=" * 60)
print("IMPORTANT: ASSIGNMENT VS MODIFICATION")
print("=" * 60)

# ========== Assignment creates a new reference ==========
print("\n1. Assignment with Immutable Types:")
a = 100
b = a
print(f"a = {a}, id(a) = {id(a)}")
print(f"b = {b}, id(b) = {id(b)}")
print(f"a is b: {a is b}")  # True - same object

a = 200  # Creates new object
print(f"\nAfter a = 200:")
print(f"a = {a}, id(a) = {id(a)}")
print(f"b = {b}, id(b) = {id(b)}")
print(f"a is b: {a is b}")  # False - different objects

# ========== Assignment with Mutable Types ==========
print("\n2. Assignment with Mutable Types:")
list1 = [1, 2, 3]
list2 = list1  # Both reference the SAME object
print(f"list1 = {list1}, id(list1) = {id(list1)}")
print(f"list2 = {list2}, id(list2) = {id(list2)}")
print(f"list1 is list2: {list1 is list2}")  # True - same object

list1.append(4)  # Modifies the shared object
print(f"\nAfter list1.append(4):")
print(f"list1 = {list1}, id(list1) = {id(list1)}")
print(f"list2 = {list2}, id(list2) = {id(list2)}")
print(f"list1 is list2: {list1 is list2}")  # Still True!
print("Both lists changed because they reference the same object!")

# ========== Creating a copy ==========
print("\n3. Creating a Copy (to avoid shared references):")
list3 = [1, 2, 3]
list4 = list3.copy()  # Creates a NEW list object
print(f"list3 = {list3}, id(list3) = {id(list3)}")
print(f"list4 = {list4}, id(list4) = {id(list4)}")
print(f"list3 is list4: {list3 is list4}")  # False - different objects

list3.append(5)
print(f"\nAfter list3.append(5):")
print(f"list3 = {list3}, id(list3) = {id(list3)}")
print(f"list4 = {list4}, id(list4) = {id(list4)}")
print("list4 unchanged because it's a separate object!")

print("\n" + "=" * 60)
print("FUNCTION ARGUMENTS: MUTABLE VS IMMUTABLE")
print("=" * 60)

def modify_immutable(x):
    """Trying to modify an immutable argument"""
    print(f"  Inside function: x = {x}, id(x) = {id(x)}")
    x = x + 10  # Creates a new object
    print(f"  After x = x + 10: x = {x}, id(x) = {id(x)} (new object)")

def modify_mutable(lst):
    """Modifying a mutable argument"""
    print(f"  Inside function: lst = {lst}, id(lst) = {id(lst)}")
    lst.append(100)  # Modifies the original object
    print(f"  After append: lst = {lst}, id(lst) = {id(lst)} (same object)")

print("\n1. Passing Immutable Type (int):")
num = 5
print(f"Before function: num = {num}, id(num) = {id(num)}")
modify_immutable(num)
print(f"After function: num = {num}, id(num) = {id(num)}")
print("Original value unchanged!")

print("\n2. Passing Mutable Type (list):")
numbers = [1, 2, 3]
print(f"Before function: numbers = {numbers}, id(numbers) = {id(numbers)}")
modify_mutable(numbers)
print(f"After function: numbers = {numbers}, id(numbers) = {id(numbers)}")
print("Original list was modified!")

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print("""
IMMUTABLE TYPES (cannot be changed after creation):
  - int, float, complex
  - str
  - tuple
  - frozenset
  - bool
  - bytes

MUTABLE TYPES (can be changed after creation):
  - list
  - dict
  - set
  - bytearray
  - user-defined classes (by default)

KEY TAKEAWAYS:
  1. Immutable objects cannot be modified in place
  2. Mutable objects can be modified, affecting all references
  3. Assignment with mutable types creates shared references
  4. Use .copy() or slicing to create independent copies
  5. Functions can modify mutable arguments (side effects!)
""")

