# bms_free

## Location
[src/backend/nodes/bitmapset.c:239-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L239-L250)

## Overview
Safely deallocates memory used by a Bitmapset, with NULL pointer protection.

## Definition

```c
void
bms_free(Bitmapset *a)
```
## Detailed Description
This function frees the memory allocated for a Bitmapset structure. It provides a safe wrapper around PostgreSQL's pfree() function by first checking if the pointer is non-NULL before attempting to free it. This prevents crashes that would occur if pfree() were called directly on a NULL pointer.

## Parameters / Member Variables
- `*a`: Pointer to the Bitmapset to be freed (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)

- Called from (representative examples):
  - [heap_update](../h/heap_update.md)
  - [bms_copy_and_free](bms_copy_and_free.md)
  - [check_index_only](../c/check_index_only.md)
  - [try_nestloop_path](../t/try_nestloop_path.md)
  - [extract_rollup_sets](../e/extract_rollup_sets.md)
  - [reduce_outer_joins_pass2](../r/reduce_outer_joins_pass2.md)
  - [RelationDestroyRelation](../R/RelationDestroyRelation.md)
  - [RelationGetIndexAttrBitmap](../R/RelationGetIndexAttrBitmap.md)

## Notes and Other Information
- Unlike standard pfree(), this function safely handles NULL input pointers
- Used extensively throughout PostgreSQL for cleanup of temporary bitmapsets
- Essential for preventing memory leaks when working with dynamically allocated bitmapsets
- The function follows PostgreSQL's naming convention for bitmapset operations with the 'bms_' prefix
- Commonly used in query optimization, join processing, and relation management code

## Simplified Source

```c
void
bms_free(Bitmapset *a)
{
    // Free the bitmapset if it's not NULL
    if (a)
        pfree(a);
}
```