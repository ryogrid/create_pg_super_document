# list_delete_int

## Location
src/backend/nodes/list.c: 891 - 909

## Overview
Deletes the first cell in an integer list that contains a value matching the specified integer datum.

## Definition
```c
List *list_delete_int(List *list, int datum)
```

## Detailed Description
This function is specifically designed for integer lists (IntList type) and searches through the list to find the first cell containing an integer value that matches the specified datum. It uses simple integer equality comparison (`==`) to find matches. The function performs a linear search from the beginning of the list, making it O(n) in time complexity.

When a matching integer is found, the cell is removed using `list_delete_cell()` and the modified list is returned. If no match is found, the original list is returned unmodified. The function includes assertions to ensure the list is a valid integer list (IntList) before processing, distinguishing it from pointer lists or OID lists.

## Parameters / Member Variables
- `list`: The IntList to search and potentially modify
- `datum`: The integer value to search for and remove from the list

## Dependencies
- Functions called/Symbols referenced:
  - IsIntegerList (macro for type checking)
  - [check_list_invariants](../c/check_list_invariants.md)
  - lfirst_int (macro to extract integer from list cell)
  - [list_delete_cell](list_delete_cell.md)
- Called from (representative examples):
  - [reorder_grouping_sets](../r/reorder_grouping_sets.md) (src/backend/optimizer/plan/planner.c:3213)

## Notes and Other Information
- Only works with integer lists (IntList), not pointer or OID lists
- Uses simple integer equality comparison for matching
- Linear search makes it unsuitable for long lists - O(n) time complexity  
- Removes only the first matching integer, not all matches
- Returns the original list unchanged if no matching integer is found
- Part of PostgreSQL's typed list API that provides type-safe operations for different data types
- The `lfirst_int()` macro safely extracts integer values from list cells