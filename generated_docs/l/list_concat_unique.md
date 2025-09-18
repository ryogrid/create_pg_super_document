# list_concat_unique

## Location
src/backend/nodes/list.c: 1405 - 1426

## Overview
Concatenates two pointer lists by appending each member of the second list to the first list, but only if it is not already present, ensuring no duplicates while preserving order.

## Definition
```c
List *list_concat_unique(List *list1, const List *list2)
```

## Detailed Description
This function performs a unique concatenation operation by appending each element from list2 to list1, but only if the element is not already present in list1. The function operates on pointer lists and uses the equal() function to determine element equality. Unlike list_union() which creates a new list, this function modifies list1 in-place, making it more memory efficient.

The function preserves the relative order of elements from list2 that are not duplicates, which is important for callers with strict ordering expectations. However, it has O(n*m) time complexity where n and m are the lengths of the lists, so it should be used carefully with large lists.

This function is commonly used in PostgreSQL's query optimizer and planner components where combining unique sets of query plan nodes or expressions is required while maintaining specific ordering constraints.

## Parameters / Member Variables
- `list1`: The target List structure that will be modified in-place by appending unique elements
- `list2`: The source List structure whose elements will be checked and potentially appended to list1

## Dependencies
- Functions called/Symbols referenced:
  - IsPointerList
  - list_member
  - check_list_invariants
- Called from (representative examples):
  - create_bitmap_subplan
  - select_active_windows
  - forfive

## Notes and Other Information
- Modifies list1 in-place rather than creating a new list
- Uses equal() function for element comparison
- Preserves relative order of non-duplicate elements from list2
- Time complexity is O(n*m) - consider alternative data structures for large lists
- Both input lists must be pointer lists (verified by assertions)
- Maintains list invariants through check_list_invariants()
- Commonly used in query optimization where maintaining ordered unique collections is crucial