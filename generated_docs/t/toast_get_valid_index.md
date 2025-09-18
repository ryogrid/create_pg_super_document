# toast_get_valid_index

## Location
src/backend/access/common/toast_internals.c: 530 - 563

## Overview
Retrieves the OID of the valid index associated with a given TOAST relation, where TOAST (The Oversized-Attribute Storage Technique) relations can only have one valid index at a time.

## Definition
```c
Oid toast_get_valid_index(Oid toastoid, LOCKMODE lock)
```

## Detailed Description
This function opens a TOAST relation and finds its valid index. TOAST relations are special PostgreSQL storage structures used to store large attribute values that exceed the page size limit. The function uses the toast_open_indexes function to locate the valid index among potentially multiple indexes, then returns the OID (object identifier) of that valid index. The function ensures proper cleanup by closing both the TOAST relation and its indexes before returning.

## Parameters / Member Variables
- `toastoid`: The OID of the TOAST relation for which to find the valid index
- `lock`: The lock mode to apply when opening the TOAST relation (typically ShareLock or AccessShareLock)

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - toast_open_indexes
  - RelationGetRelid
  - toast_close_indexes
  - table_close
- Called from (representative examples):
  - swap_relation_files (in cluster.c)
  - finish_heap_swap (in cluster.c)

## Notes and Other Information
- TOAST relations can have only one valid index at any given time, making this function essential for index management operations
- The function uses NoLock when closing relations since it only needs to hold the lock during the search operation
- This is commonly used during table clustering and relation swapping operations where TOAST index information needs to be preserved
- The function is part of PostgreSQL's internal TOAST management system and handles the complexity of index validation automatically