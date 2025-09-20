# ItemPointerEquals

## Location
[src/backend/storage/page/itemptr.c:35-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/itemptr.c#L35-L50)

## Overview
ItemPointerEquals is a utility function that compares two ItemPointer structures to determine if they point to the same physical location (same block and offset) within a PostgreSQL heap file.

## Definition

```c
bool
ItemPointerEquals(ItemPointer pointer1, ItemPointer pointer2)
```
## Detailed Description
This function performs equality comparison between two ItemPointer structures by checking if both the block number and offset number components are identical. ItemPointers are fundamental data structures in PostgreSQL that represent physical locations of tuples within heap files. The function returns true only when both pointers reference the exact same tuple location, making it essential for tuple identification and comparison operations throughout the database system.

The function uses accessor macros to extract and compare the block and offset components rather than performing direct memory comparison, ensuring proper handling of the ItemPointer's internal structure.

## Parameters / Member Variables
- : First ItemPointer to compare - must be a valid ItemPointer structure
- : Second ItemPointer to compare - must be a valid ItemPointer structure

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumber](ItemPointerGetBlockNumber.md): Extracts the block number component from an ItemPointer
  - [ItemPointerGetOffsetNumber](ItemPointerGetOffsetNumber.md): Extracts the offset number component from an ItemPointer
- Called from (representative examples):
  - [heap_get_latest_tid](../h/heap_get_latest_tid.md): Used in tuple version chain traversal
  - [heap_delete](../h/heap_delete.md): Used to verify tuple identity before deletion
  - [heap_update](../h/heap_update.md): Used to verify tuple identity before updates
  - [index_getnext_slot](../i/index_getnext_slot.md): Used in index scan operations
  - [SearchCatCacheList](../S/SearchCatCacheList.md): Used in system catalog cache operations

## Notes and Other Information
- The function asserts that both disk item pointers are valid before performing the comparison
- This is a core utility function used extensively throughout PostgreSQL's storage and access method implementations
- The function is critical for maintaining tuple identity during MVCC operations
- Used heavily in heap operations, index operations, and caching mechanisms
- Performance is optimized by using inline accessor macros rather than function calls