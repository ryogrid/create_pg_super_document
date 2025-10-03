# ItemPointerIsValid

## Location
[src/include/storage/itemptr.h:83-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/itemptr.h#L83-L92)

## Overview
Validates whether a disk item pointer is valid by checking if the pointer is not NULL and the position ID is non-zero.

## Definition

```c
static inline bool
ItemPointerIsValid(const ItemPointerData *pointer)
```
## Detailed Description
ItemPointerIsValid is a utility function that determines whether an ItemPointerData structure represents a valid disk item pointer. The function performs two key validations: first, it ensures the pointer itself is not NULL using PointerIsValid, and second, it verifies that the ip_posid field is not zero, which would indicate an invalid or uninitialized item pointer.

This function is critical for ensuring data integrity when working with heap tuples and index entries, as invalid item pointers can lead to accessing non-existent or corrupted data.

## Parameters / Member Variables
- `*pointer`: A pointer to an ItemPointerData structure to be validated
## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
- Called from (representative examples):
  - [heap_get_latest_tid](../h/heap_get_latest_tid.md)
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)
  - [HeapTupleSatisfiesMVCC](../H/HeapTupleSatisfiesMVCC.md)
  - [index_getnext_tid](../i/index_getnext_tid.md)
  - [ItemPointerGetBlockNumber](ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](ItemPointerGetOffsetNumber.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- The function is widely used throughout the PostgreSQL codebase, particularly in heap access methods, index operations, and trigger execution
- Returns true only if both conditions are met: the pointer is valid AND the position ID is non-zero
- Essential for preventing segmentation faults and data corruption when accessing heap tuples and index entries

## Simplified Source

```c
static inline bool ItemPointerIsValid(const ItemPointerData *pointer)
{
    return PointerIsValid(pointer) && pointer->ip_posid != 0;
}
```