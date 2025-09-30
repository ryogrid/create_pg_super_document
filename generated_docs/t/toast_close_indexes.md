# toast_close_indexes

## Location
[src/backend/access/common/toast_internals.c:623-640](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_internals.c#L623-L640)

## Overview
Closes an array of TOAST indexes and frees the associated memory, designed to be used as the cleanup companion to toast_open_indexes.

## Definition
```c
void toast_close_indexes(Relation *toastidxs, int num_indexes, LOCKMODE lock)
```

## Detailed Description
This function provides proper cleanup for indexes opened by toast_open_indexes. It iterates through the array of index relations, closes each one using index_close with the specified lock mode, and then frees the memory allocated for the indexes array using pfree. This is essential for proper resource management in PostgreSQL's TOAST system, ensuring that opened indexes don't leak resources.

## Parameters / Member Variables
- `toastidxs`: Array of index relations to be closed (previously opened by toast_open_indexes)
- `num_indexes`: Number of indexes in the array
- `lock`: The lock mode to use when closing the indexes (typically NoLock for most cleanup scenarios)

## Dependencies
- Functions called/Symbols referenced:
  - [index_close](../i/index_close.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [toast_save_datum](toast_save_datum.md) (in toast_internals.c)
  - [toast_delete_datum](toast_delete_datum.md) (in toast_internals.c)
  - [toastrel_valueid_exists](toastrel_valueid_exists.md) (in toast_internals.c)
  - [toast_get_valid_index](toast_get_valid_index.md) (in toast_internals.c)
  - [heap_fetch_toast_slice](../h/heap_fetch_toast_slice.md) (in heaptoast.c)

## Notes and Other Information
- This function is the required cleanup counterpart to toast_open_indexes and should always be called after using opened TOAST indexes
- The function automatically handles memory deallocation for the indexes array, so callers should not attempt to free the array themselves
- Usually called with NoLock since the indexes were opened for read operations and cleanup doesn't require additional locking
- Part of PostgreSQL's resource management discipline where every open operation has a corresponding close operation
- Simple but critical function that prevents memory leaks in TOAST operations

## Simplified Source

```c
void
toast_close_indexes(Relation *toastidxs, int num_indexes, LOCKMODE lock)
{
    // Close each index and free the array
    for (int i = 0; i < num_indexes; i++)
        index_close(toastidxs[i], lock);
    pfree(toastidxs);
}
```