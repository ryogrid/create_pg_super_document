# ItemPointerGetOffsetNumber

## Location
[src/include/storage/itemptr.h:124-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/itemptr.h#L124-L134)

## Overview
Safely extracts the offset number from an ItemPointerData structure with validity checking enabled.

## Definition
static inline OffsetNumber ItemPointerGetOffsetNumber(const ItemPointerData *pointer)

## Detailed Description
ItemPointerGetOffsetNumber is the safe version of the offset number extraction function that includes validity checking. Before extracting the offset number, it uses an Assert to verify that the item pointer is valid via ItemPointerIsValid. If the assertion passes, it delegates to ItemPointerGetOffsetNumberNoCheck to perform the actual extraction. The offset number represents the position of a tuple within a specific page, which is crucial for precisely locating data within PostgreSQL's storage system.

This function provides the same safety-performance balance as its block number counterpart - assertions are compiled out in production builds while providing valuable debugging checks during development.

## Parameters / Member Variables
- pointer: A pointer to an ItemPointerData structure from which to extract the offset number

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerIsValid](ItemPointerIsValid.md)
  - [ItemPointerGetOffsetNumberNoCheck](ItemPointerGetOffsetNumberNoCheck.md)
- Called from (representative examples):
  - [heap_fetch](../h/heap_fetch.md)
  - [heap_insert](../h/heap_insert.md)
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)
  - [brininsert](../b/brininsert.md)
  - [TidStoreIsMember](../T/TidStoreIsMember.md)
  - [spgTestLeafTuple](../s/spgTestLeafTuple.md)
  - [ItemPointerEquals](ItemPointerEquals.md)

## Notes and Other Information
- This is an inline function for performance optimization
- Uses Assert for validity checking, which is compiled out in production builds
- This is the checked version - recommended for general use over the NoCheck variant
- Extensively used throughout PostgreSQL for safe offset number access
- Returns an OffsetNumber type representing the tuple position within a page
- The Assert provides early detection of invalid pointer usage during development
- OffsetNumber values start from 1 for valid tuples, with 0 typically indicating invalid or special cases

## Simplified Source

```c
// Simplified version of ItemPointerGetOffsetNumber
static inline OffsetNumber
ItemPointerGetOffsetNumber(const ItemPointerData *pointer)
{
    // Verify pointer is valid (compiled out in production)
    Assert(ItemPointerIsValid(pointer));

    // Extract offset number using unchecked version
    return ItemPointerGetOffsetNumberNoCheck(pointer);
}
```

Key simplifications made:
- Added explanatory comments for the two main operations
- Clarified that Assert is compiled out in production builds
- Maintained the simple structure since the original is already quite minimal
- Preserved the essential safety-then-extract pattern