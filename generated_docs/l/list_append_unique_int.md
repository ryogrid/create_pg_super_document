# list_append_unique_int

## Location
src/backend/nodes/list.c: 1368 - 1379

## Overview
Appends an integer value to a list only if it is not already present, ensuring uniqueness of integer values in the list.

## Definition


## Detailed Description
This function is a specialized variant of list_append_unique() that operates specifically on lists of integers. It checks whether the given integer value already exists in the list using list_member_int(), and only appends the value if it's not already present. This ensures that the resulting list contains only unique integer values, preventing duplicates while maintaining the original order of elements.

The function is part of PostgreSQL's generic list infrastructure and provides type-safe operations for integer lists, which are commonly used throughout the PostgreSQL codebase for managing collections of object IDs, indexes, and other integer-based identifiers.

## Parameters / Member Variables
- `list`: The target List structure to which the integer should be appended
- `datum`: The integer value to be added to the list (only if not already present)

## Dependencies
- Functions called/Symbols referenced:
  - [list_member_int](list_member_int.md)
  - lappend_int
- Called from (representative examples):
  - forfive

## Notes and Other Information
- Returns the original list if the integer value is already present
- Returns a new list with the integer appended if the value is unique
- This function is commonly used in PostgreSQL's planner and optimizer components where maintaining unique sets of integer identifiers is crucial
- Part of the generic List API that provides type-safe operations for different data types