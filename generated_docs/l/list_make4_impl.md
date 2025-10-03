# list_make4_impl

## Location
[src/backend/nodes/list.c:270-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L270-L283)

## Overview
Creates a new PostgreSQL list with exactly four elements, serving as the core implementation for the list_make4 family of macros.

## Definition

```c
List *
list_make4_impl(NodeTag t, ListCell datum1, ListCell datum2,
				ListCell datum3, ListCell datum4)
```
## Detailed Description
The list_make4_impl function is the underlying implementation for creating lists with four elements in PostgreSQL's list system. It allocates a new list with a fixed capacity of 4 elements and directly assigns the provided ListCell values to the first four positions in the list's elements array. This function is part of PostgreSQL's type-safe list creation system and is typically called through convenience macros rather than directly.

The function ensures list integrity by calling check_list_invariants before returning, which validates the internal consistency of the newly created list structure.

## Parameters / Member Variables
- `t`: NodeTag specifying the type of nodes this list will contain (e.g., T_List for generic lists, T_IntList for integer lists)
- `datum1`: First ListCell value to be stored at position 0 in the list
- `datum2`: Second ListCell value to be stored at position 1 in the list
- `datum3`: Third ListCell value to be stored at position 2 in the list
- `datum4`: Fourth ListCell value to be stored at position 3 in the list
## Dependencies
- Functions called/Symbols referenced:
  - [new_list](../n/new_list.md) (creates the base list structure with specified capacity)
  - [check_list_invariants](../c/check_list_invariants.md) (validates list consistency)
- Called from (representative examples):
  - list_make4 (generic four-element list macro)
  - list_make4_int (four-element integer list macro)
  - list_make4_oid (four-element OID list macro)
  - list_make4_xid (four-element transaction ID list macro)
  - forfive (iterator macro for processing multiple lists)

## Notes and Other Information
- This function is part of a family of list_makeN_impl functions (N=1,2,3,4,5) that create lists with fixed numbers of elements
- The function assumes the caller has properly initialized the ListCell values with appropriate data for the specified NodeTag type
- Direct calls to this function are discouraged in favor of using the type-safe macros like list_make4, list_make4_int, etc.
- The list capacity is fixed at creation time to exactly 4 elements, making this efficient for small, known-size lists