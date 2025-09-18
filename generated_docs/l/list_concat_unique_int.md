# list_concat_unique_int

## Location
src/backend/nodes/list.c: 1448 - 1468

## Overview
Concatenates two integer lists by appending each integer from the second list to the first list, but only if it is not already present, ensuring no duplicate integers.

## Definition
```c
List *list_concat_unique_int(List *list1, const List *list2)
```

## Detailed Description
This function is a specialized variant of list_concat_unique() that operates specifically on lists of integers. It performs an in-place concatenation by appending each integer value from list2 to list1, but only if that integer is not already present in list1. The function uses list_member_int() for efficient integer membership testing and lappend_int() for type-safe integer appending.

This function is optimized for integer operations and provides better performance compared to the generic list_concat_unique() when working with integer collections. It maintains the relative order of non-duplicate elements from list2 and is commonly used in PostgreSQL's internals for managing collections of object IDs, column numbers, and other integer-based identifiers where uniqueness is required.

## Parameters / Member Variables
- `list1`: The target List structure containing integers that will be modified in-place
- `list2`: The source List structure containing integers whose unique elements will be appended to list1

## Dependencies
- Functions called/Symbols referenced:
  - IsIntegerList
  - list_member_int
  - lfirst_int
  - lappend_int
  - check_list_invariants
- Called from (representative examples):
  - forfive

## Notes and Other Information
- Operates specifically on integer lists with type safety enforced by assertions
- Modifies list1 in-place rather than creating a new list
- Uses integer-specific functions for optimal performance
- Preserves the relative order of non-duplicate elements from list2
- Both input lists must be integer lists (verified by IsIntegerList assertions)
- Maintains list invariants through check_list_invariants()
- Part of PostgreSQL's type-safe list API for efficient integer collection management
- Commonly used for maintaining unique sets of column indices, attribute numbers, and object identifiers