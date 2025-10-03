# list_make3_impl

## Location
[src/backend/nodes/list.c:257-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L257-L269)

## Overview
Creates a new PostgreSQL list with exactly three elements, serving as the core implementation for the list_make3 family of macros.

## Definition

```c
List *
list_make3_impl(NodeTag t, ListCell datum1, ListCell datum2,
				ListCell datum3)
```
## Detailed Description
The list_make3_impl function is the underlying implementation for creating lists with three elements in PostgreSQL's list system. It allocates a new list with a fixed capacity of 3 elements and directly assigns the provided ListCell values to the first three positions in the list's elements array. This function is part of PostgreSQL's type-safe list creation system and is typically called through convenience macros rather than directly.

The function ensures list integrity by calling check_list_invariants before returning, which validates the internal consistency of the newly created list structure.

## Parameters / Member Variables
- `t`: NodeTag specifying the type of nodes this list will contain (e.g., T_List for generic lists, T_IntList for integer lists)
- `datum1`: First ListCell value to be stored at position 0 in the list
- `datum2`: Second ListCell value to be stored at position 1 in the list
- `datum3`: Third ListCell value to be stored at position 2 in the list
## Dependencies
- Functions called/Symbols referenced:
  - [new_list](../n/new_list.md) (creates the base list structure with specified capacity)
  - [check_list_invariants](../c/check_list_invariants.md) (validates list consistency)
- Called from (representative examples):
  - list_make3 (generic three-element list macro)
  - list_make3_int (three-element integer list macro)
  - list_make3_oid (three-element OID list macro)
  - list_make3_xid (three-element transaction ID list macro)
  - forfive (iterator macro for processing multiple lists)

## Notes and Other Information
- This function is part of a family of list_makeN_impl functions (N=1,2,3,4,5) that create lists with fixed numbers of elements
- The function assumes the caller has properly initialized the ListCell values with appropriate data for the specified NodeTag type
- Direct calls to this function are discouraged in favor of using the type-safe macros like list_make3, list_make3_int, etc.
- The list capacity is fixed at creation time to exactly 3 elements, making this efficient for small, known-size lists