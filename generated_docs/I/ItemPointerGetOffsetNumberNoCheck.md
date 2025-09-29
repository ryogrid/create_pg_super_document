# ItemPointerGetOffsetNumberNoCheck

## Location
[src/include/storage/itemptr.h:114-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/itemptr.h#L114-L123)

## Overview
Extracts the offset number from an ItemPointerData structure without performing validity checks.

## Definition
static inline OffsetNumber ItemPointerGetOffsetNumberNoCheck(const ItemPointerData *pointer)

## Detailed Description
ItemPointerGetOffsetNumberNoCheck is a low-level utility function that directly accesses the ip_posid field of an ItemPointerData structure to retrieve the offset number. This function performs no validation and simply returns the raw offset value stored in the pointer. The offset number represents the position of a tuple within a specific page/block, typically ranging from 1 to the maximum number of items that can fit on a page.

Like other NoCheck functions, this is designed for performance-critical scenarios where pointer validity has already been established or where raw access to the offset is needed regardless of validity.

## Parameters / Member Variables
- pointer: A pointer to an ItemPointerData structure from which to extract the offset number

## Dependencies
- Functions called/Symbols referenced:
  - (None - direct field access)
- Called from (representative examples):
  - [ItemPointerCompare](ItemPointerCompare.md)
  - [ItemPointerInc](ItemPointerInc.md)
  - [ItemPointerDec](ItemPointerDec.md)
  - [table_tuple_get_latest_tid](../t/table_tuple_get_latest_tid.md)
  - [BTreeTupleIsPivot](../B/BTreeTupleIsPivot.md)
  - [ItemPointerGetOffsetNumber](ItemPointerGetOffsetNumber.md)
  - GinItemPointerGetOffsetNumber

## Notes and Other Information
- This is an inline function for maximum performance
- Directly accesses the ip_posid field without any safety checks
- The NoCheck suffix indicates this is the unchecked version
- Returns an OffsetNumber type representing the tuple position within a page
- Used extensively in tuple comparison and manipulation functions
- OffsetNumber values typically start from 1 (not 0) for valid tuples

## Simplified Source

```c
// Get offset number from ItemPointer without validation
static inline OffsetNumber
ItemPointerGetOffsetNumberNoCheck(const ItemPointerData *pointer)
{
    return pointer->ip_posid;
}
```