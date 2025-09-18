# toast_open_indexes

## Location
src/backend/access/common/toast_internals.c: 564 - 622

## Overview
Opens all indexes associated with a given TOAST relation and returns an array of these indexes along with the position of the valid index used by the TOAST relation.

## Definition
```c
int toast_open_indexes(Relation toastrel, LOCKMODE lock, Relation **toastidxs, int *num_indexes)
```

## Detailed Description
This function retrieves and opens all indexes associated with a TOAST relation. It first gets the list of indexes using RelationGetIndexList, then opens each index with the specified lock mode. The function searches through the opened indexes to find the first valid index (where indisvalid is true) and returns its position in the array. The caller is responsible for closing the indexes and freeing the allocated memory. If no valid index is found, the function raises an ERROR, as every TOAST relation must have exactly one valid index.

## Parameters / Member Variables
- `toastrel`: The TOAST relation for which to open indexes
- `lock`: The lock mode to apply when opening the indexes
- `toastidxs`: Output parameter - pointer to array of opened index relations
- `num_indexes`: Output parameter - pointer to the number of indexes found

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetIndexList
  - index_open
  - list_free
  - lfirst_oid
  - palloc
  - elog
  - RelationGetRelid
- Called from (representative examples):
  - toast_save_datum (in toast_internals.c)
  - toast_delete_datum (in toast_internals.c)
  - toastrel_valueid_exists (in toast_internals.c)
  - toast_get_valid_index (in toast_internals.c)
  - heap_fetch_toast_slice (in heaptoast.c)

## Notes and Other Information
- The function allocates memory for the index array using palloc, and the caller must free this memory
- TOAST relations should have exactly one valid index; the function will error if no valid index is found
- The valid index is identified by the indisvalid flag in the index's rd_index structure
- This is a core function in PostgreSQL's TOAST system, used whenever TOAST data needs to be accessed or modified
- The function returns the position (index) of the valid index within the opened indexes array, not the OID