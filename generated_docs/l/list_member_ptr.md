# list_member_ptr

## Location
[src/backend/nodes/list.c:682-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L682-L701)

## Overview
Tests whether a given pointer value is a member of a pointer list using simple pointer comparison for equality determination.

## Definition

```c
bool
list_member_ptr(const List *list, const void *datum)
```
## Detailed Description
The  function performs membership testing on PostgreSQL's List data structure specifically for pointer lists. It iterates through the list cells using the  macro and compares each cell's data pointer with the target datum using direct pointer comparison ( operator). The function includes assertions to ensure the input list is actually a pointer list type and validates list invariants for debugging purposes.

This function is optimized for pointer comparison and should only be used with lists that contain pointer values. For other data types, use the appropriate variant functions like  or .

## Parameters / Member Variables
- : A constant pointer to the List structure to search within. Must be a pointer list type.
- : A constant void pointer representing the target value to search for in the list.

## Dependencies
- Functions called/Symbols referenced:
  - IsPointerList - Validates that the list contains pointer values
  - [check_list_invariants](../c/check_list_invariants.md) - Performs debugging validation of list structure
  - foreach - Macro for iterating through list cells
  - lfirst - Macro for accessing the data pointer of a list cell

- Called from (representative examples):
  - [ExecInsert](../E/ExecInsert.md) - Used in executor for insert operations
  - [list_union_ptr](list_union_ptr.md) - Used when creating union of pointer lists
  - [list_difference_ptr](list_difference_ptr.md) - Used when computing difference between pointer lists
  - [list_append_unique_ptr](list_append_unique_ptr.md) - Used to ensure uniqueness when appending pointers
  - [get_foreign_key_join_selectivity](../g/get_foreign_key_join_selectivity.md) - Used in query optimization
  - [create_bitmap_scan_plan](../c/create_bitmap_scan_plan.md) - Used in plan creation

## Notes and Other Information
- The function uses simple pointer address comparison, not content comparison
- Only suitable for pointer lists; will assert if used with other list types
- Returns  if the pointer is found,  otherwise
- Part of PostgreSQL's generic List API located in src/backend/nodes/list.c
- Commonly used throughout the system for checking membership in collections of pointers to structures, nodes, or other objects