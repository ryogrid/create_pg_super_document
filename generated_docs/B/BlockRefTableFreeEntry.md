# BlockRefTableFreeEntry

## Location
[src/common/blkreftable.c:1122-1151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L1122-L1151)

## Overview
Releases all memory allocated for a BlockRefTableEntry that was previously created by CreateBlockRefTableEntry.

## Definition
```c
void BlockRefTableFreeEntry(BlockRefTableEntry *entry)
```

## Detailed Description
This function performs complete cleanup of a BlockRefTableEntry by freeing all dynamically allocated memory associated with it. It systematically releases the chunk_size array, chunk_usage array, chunk_data array, and finally the entry structure itself. The function sets pointers to NULL after freeing them to prevent potential double-free errors, following PostgreSQL's memory management best practices.

This is a complementary function to CreateBlockRefTableEntry and should be called when a BlockRefTableEntry is no longer needed to prevent memory leaks.

## Parameters / Member Variables
- `entry`: Pointer to the BlockRefTableEntry structure to be freed, including all its dynamically allocated internal arrays

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - Functions that need to clean up BlockRefTableEntry structures

## Notes and Other Information
- Must be called for every BlockRefTableEntry created by CreateBlockRefTableEntry to avoid memory leaks
- Sets all freed pointers to NULL to prevent accidental reuse
- Safe to call even if some internal arrays are NULL
- Part of the standard create/destroy pattern for BlockRefTableEntry lifecycle management
- Should be called before the containing structure is freed or goes out of scope

## Simplified Source

```c
void
BlockRefTableFreeEntry(BlockRefTableEntry *entry)
{
    // Free chunk arrays if allocated
    if (entry->chunk_size != NULL) {
        pfree(entry->chunk_size);
        entry->chunk_size = NULL;
    }

    if (entry->chunk_usage != NULL) {
        pfree(entry->chunk_usage);
        entry->chunk_usage = NULL;
    }

    if (entry->chunk_data != NULL) {
        pfree(entry->chunk_data);
        entry->chunk_data = NULL;
    }

    // Free the entry structure itself
    pfree(entry);
}
```