# ItemPointerCompare

## Location
[src/backend/storage/page/itemptr.c:51-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/itemptr.c#L51-L83)

## Overview
ItemPointerCompare is a generic comparison function that provides btree-style ordering for ItemPointer structures, returning -1, 0, or 1 to indicate less-than, equal-to, or greater-than relationships.

## Definition

```c
int32
ItemPointerCompare(ItemPointer arg1, ItemPointer arg2)
```
## Detailed Description
This function implements a lexicographic comparison of ItemPointer structures, first comparing block numbers and then offset numbers within blocks. The comparison follows standard btree ordering semantics, making it suitable for use in sorting operations, btree index operations, and general ordering requirements. The function uses "NoCheck" variants of accessor functions to handle potentially invalid ItemPointers (such as user-supplied TIDs) without triggering assertions.

The comparison logic prioritizes block numbers (physical storage blocks) over offset numbers (tuple positions within blocks), ensuring that tuples from earlier blocks always sort before tuples from later blocks, regardless of their offset positions.

## Parameters / Member Variables
- : First ItemPointer to compare - may be user-supplied and potentially invalid
- : Second ItemPointer to compare - may be user-supplied and potentially invalid

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumberNoCheck](ItemPointerGetBlockNumberNoCheck.md): Safely extracts block number without validation assertions
  - [ItemPointerGetOffsetNumberNoCheck](ItemPointerGetOffsetNumberNoCheck.md): Safely extracts offset number without validation assertions
- Called from (representative examples):
  - [bttidcmp](../b/bttidcmp.md): Used in btree comparison operations for TID data types
  - [heap_set_tidrange](../h/heap_set_tidrange.md): Used in heap tuple range scanning operations
  - [_bt_compare](../b/_bt_compare.md): Used in btree search and insertion operations
  - [TidRangeEval](../T/TidRangeEval.md): Used in TID range scan execution
  - [tideq](../t/tideq.md), tidne, tidlt, tidle, tidgt, tidge: Used in TID comparison operators

## Notes and Other Information
- Returns -1 if arg1 < arg2, 0 if arg1 == arg2, and 1 if arg1 > arg2
- Uses NoCheck variants to avoid assertions on potentially invalid user-supplied TIDs
- Essential for btree operations on TID data types and internal tuple ordering
- The comparison is stable and transitive, making it suitable for sorting algorithms
- Block number comparison takes precedence over offset number comparison
- Widely used in both user-facing TID operations and internal storage management

## Simplified Source
```c
int32
ItemPointerCompare(ItemPointer arg1, ItemPointer arg2)
{
    // Extract block numbers for comparison
    BlockNumber b1 = ItemPointerGetBlockNumberNoCheck(arg1);
    BlockNumber b2 = ItemPointerGetBlockNumberNoCheck(arg2);

    // Compare block numbers first
    if (b1 < b2)
        return -1;
    else if (b1 > b2)
        return 1;

    // Blocks are equal, compare offsets
    OffsetNumber off1 = ItemPointerGetOffsetNumberNoCheck(arg1);
    OffsetNumber off2 = ItemPointerGetOffsetNumberNoCheck(arg2);

    if (off1 < off2)
        return -1;
    else if (off1 > off2)
        return 1;
    else
        return 0;
}
```