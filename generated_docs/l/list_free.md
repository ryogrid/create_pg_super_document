# list_free

## Location
[src/backend/nodes/list.c:1546-1559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1546-L1559)

## Overview
Frees all memory associated with a List structure while leaving the pointed-to elements intact.

## Definition

```c
void
list_free(List *list)
```
## Detailed Description
This function performs a shallow free operation on a PostgreSQL List structure. It deallocates the memory used by the list itself (including any dynamically allocated elements array) but does NOT free the objects that the list elements point to. This is the standard way to free a list when the caller is responsible for managing the memory of the individual elements or when those elements are allocated elsewhere and should remain valid.

The function is implemented as a simple wrapper around `list_free_private(list, false)`, where the false parameter indicates that a shallow free should be performed. After calling this function, the list pointer becomes invalid and the caller should set it to NIL for safety.

## Parameters / Member Variables
- `list`: The List structure to free. The list itself and its internal storage will be deallocated, but pointed-to elements remain untouched.

## Dependencies
- Functions called/Symbols referenced:
  - [list_free_private](list_free_private.md)
- Called from (representative examples):
  - [toast_open_indexes](../t/toast_open_indexes.md)
  - [heap_truncate_find_FKs](../h/heap_truncate_find_FKs.md)  
  - [ExecOpenIndices](../E/ExecOpenIndices.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - ProcessGUCArray

## Notes and Other Information
- This function performs a shallow free - only the list structure is freed, not the elements it points to
- The caller is advised to set the list pointer to NIL after calling this function for safety
- Widely used throughout PostgreSQL for cleaning up temporary lists where element memory is managed separately
- Counterpart to `list_free_deep()` which also frees the pointed-to elements
- Part of PostgreSQL's standard memory management practices for list data structures
- Used extensively in index operations, relation caching, and configuration processing