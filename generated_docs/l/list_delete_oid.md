# list_delete_oid

## Location
[src/backend/nodes/list.c:910-942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L910-L942)

## Overview
Deletes the first cell in an OID list that contains a value matching the specified OID datum.

## Definition
```c
List *list_delete_oid(List *list, Oid datum)
```

## Detailed Description
This function is specifically designed for OID lists (OidList type) and searches through the list to find the first cell containing an OID (Object Identifier) value that matches the specified datum. PostgreSQL uses OIDs extensively to identify database objects like tables, functions, types, etc. The function uses simple OID equality comparison to find matches and performs a linear search from the beginning of the list.

When a matching OID is found, the cell is removed using `list_delete_cell()` and the modified list is returned. If no match is found, the original list is returned unmodified. The function includes assertions to ensure the list is a valid OID list (OidList) before processing, distinguishing it from pointer lists or integer lists.

## Parameters / Member Variables
- `list`: The OidList to search and potentially modify
- `datum`: The OID value to search for and remove from the list

## Dependencies
- Functions called/Symbols referenced:
  - IsOidList (macro for type checking)
  - [check_list_invariants](../c/check_list_invariants.md)
  - lfirst_oid (macro to extract OID from list cell)
  - [list_delete_cell](list_delete_cell.md)
- Called from (representative examples):
  - [RemoveReindexPending](../R/RemoveReindexPending.md) (src/backend/catalog/index.c:4143)

## Notes and Other Information
- Only works with OID lists (OidList), not pointer or integer lists
- Uses simple OID equality comparison for matching
- Linear search makes it unsuitable for long lists - O(n) time complexity
- Removes only the first matching OID, not all matches  
- Returns the original list unchanged if no matching OID is found
- Part of PostgreSQL's typed list API for managing collections of database object identifiers
- The `lfirst_oid()` macro safely extracts OID values from list cells
- Commonly used in catalog operations and object management where OID tracking is required