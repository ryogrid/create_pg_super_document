# list_last_cell

## Location
[src/include/nodes/pg_list.h:288-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pg_list.h#L288-L298)

## Overview
Returns a pointer to the last cell in a non-empty List, providing direct access to the final element for efficient list operations.

## Definition

```c
static inline ListCell *
list_last_cell(const List *list)
```
## Detailed Description
The `list_last_cell` function is an inline utility that returns a pointer to the last ListCell in a given List. It uses direct array indexing to access the last element efficiently by calculating `list->elements[list->length - 1]`. This function is designed for performance-critical code paths where direct access to the last cell is needed without the overhead of traversing the entire list.

The function includes an assertion to ensure the list is not NIL (empty), making it a safe utility for non-empty lists. It's commonly used as a building block for other list manipulation functions that need access to the last element.

## Parameters / Member Variables
- `list`: A const pointer to the List structure from which to retrieve the last cell. Must be non-NIL.

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for runtime validation)
  - [List](../L/List.md) structure (accesses length and elements fields)
- Called from (representative examples):
  - llast (wrapper function for getting last element value)
  - llast_int (wrapper for integer values)
  - llast_oid (wrapper for OID values)
  - llast_xid (wrapper for transaction ID values)

## Notes and Other Information
- This is a static inline function for optimal performance
- Requires the list to be non-empty; calling with NIL will trigger an assertion failure
- Directly accesses the internal array structure of PostgreSQL's List implementation
- Part of the core list manipulation API used throughout the PostgreSQL codebase
- The function provides O(1) access time to the last element, unlike traditional linked list implementations