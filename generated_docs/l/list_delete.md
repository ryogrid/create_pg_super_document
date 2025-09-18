# list_delete

## Location
src/backend/nodes/list.c: 853 - 871

## Overview
Deletes the first cell in a list that contains a datum matching the specified value, using PostgreSQL's generic equality comparison.

## Definition
```c
List *list_delete(List *list, void *datum)
```

## Detailed Description
This function searches through a pointer list to find the first cell containing a datum that equals the specified value and removes it. Equality is determined using PostgreSQL's `equal()` function, which provides deep comparison for complex data structures. The search is performed linearly from the beginning of the list, so this function should be avoided for long lists due to its O(n) time complexity.

If a matching datum is found, the cell is removed using `list_delete_cell()` and the modified list is returned. If no match is found, the original list is returned unmodified. The function includes assertions to ensure the list is a valid pointer list (as opposed to integer or OID lists) before processing.

## Parameters / Member Variables
- `list`: The List to search and potentially modify
- `datum`: Pointer to the data value to search for and remove from the list

## Dependencies
- Functions called/Symbols referenced:
  - IsPointerList (macro for type checking)
  - [check_list_invariants](../c/check_list_invariants.md)
  - [equal](../e/equal.md) (PostgreSQL's generic equality function)
  - [list_delete_cell](list_delete_cell.md)
- Called from (representative examples):
  - [check_publications](../c/check_publications.md) (src/backend/commands/subscriptioncmds.c:522)
  - [unregister_ENR](../u/unregister_ENR.md) (src/backend/utils/misc/queryenvironment.c:88) 
  - [injection_points_detach](../i/injection_points_detach.md) (src/test/modules/injection_points/injection_points.c:399)

## Notes and Other Information
- Only works with pointer lists, not integer or OID lists
- Uses linear search - O(n) time complexity makes it unsuitable for long lists
- Removes only the first matching element, not all matches
- The `equal()` function provides deep structural comparison suitable for PostgreSQL node types
- Returns the original list unchanged if no matching datum is found
- Part of PostgreSQL's generic list API used throughout the codebase