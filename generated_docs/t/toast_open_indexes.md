# toast_open_indexes

## Location
[src/backend/access/common/toast_internals.c:564-622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_internals.c#L564-L622)

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
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [index_open](../i/index_open.md)
  - [list_free](../l/list_free.md)
  - lfirst_oid
  - [palloc](../p/palloc.md)
  - elog
  - RelationGetRelid
- Called from (representative examples):
  - [toast_save_datum](toast_save_datum.md) (in toast_internals.c)
  - [toast_delete_datum](toast_delete_datum.md) (in toast_internals.c)
  - [toastrel_valueid_exists](toastrel_valueid_exists.md) (in toast_internals.c)
  - [toast_get_valid_index](toast_get_valid_index.md) (in toast_internals.c)
  - [heap_fetch_toast_slice](../h/heap_fetch_toast_slice.md) (in heaptoast.c)

## Notes and Other Information
- The function allocates memory for the index array using palloc, and the caller must free this memory
- TOAST relations should have exactly one valid index; the function will error if no valid index is found
- The valid index is identified by the indisvalid flag in the index's rd_index structure
- This is a core function in PostgreSQL's TOAST system, used whenever TOAST data needs to be accessed or modified
- The function returns the position (index) of the valid index within the opened indexes array, not the OID

## Simplified Source

```c
int
toast_open_indexes(Relation toastrel, LOCKMODE lock,
                   Relation **toastidxs, int *num_indexes)
{
    // Get list of indexes for toast relation
    List *indexlist = RelationGetIndexList(toastrel);
    Assert(indexlist != NIL);

    *num_indexes = list_length(indexlist);

    // Open all indexes
    *toastidxs = (Relation *) palloc(*num_indexes * sizeof(Relation));
    int i = 0;
    ListCell *lc;
    foreach(lc, indexlist)
        (*toastidxs)[i++] = index_open(lfirst_oid(lc), lock);

    // Find first valid index
    int valid_index = 0;
    bool found = false;
    for (i = 0; i < *num_indexes; i++)
    {
        if ((*toastidxs)[i]->rd_index->indisvalid)
        {
            valid_index = i;
            found = true;
            break;
        }
    }

    list_free(indexlist);

    if (!found)
        elog(ERROR, "no valid index found for toast relation with Oid %u",
             RelationGetRelid(toastrel));

    return valid_index;
}
```