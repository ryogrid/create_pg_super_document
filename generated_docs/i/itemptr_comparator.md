# itemptr_comparator

## Location
[src/backend/executor/nodeTidscan.c:283-311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidscan.c#L283-L311)

## Overview
itemptr_comparator is a static comparison function used for sorting ItemPointerData (TID) values in ascending order by block number first, then by offset number within the block.

## Definition
```c
static int itemptr_comparator(const void *a, const void *b)
```

## Detailed Description
This function implements a comparator for qsort operations on arrays of ItemPointerData structures (TIDs). It establishes a total ordering based on the physical location of tuples within the database storage.

The comparison logic follows a hierarchical approach:
1. **Primary ordering**: Block number (ba vs bb) - tuples in lower-numbered blocks come first
2. **Secondary ordering**: Offset number (oa vs ob) - within the same block, tuples with lower offset numbers come first

This ordering ensures that when TIDs are sorted using this comparator, they will be arranged in a way that optimizes sequential access patterns during heap scans, as tuples will be visited in their physical storage order.

The function returns standard qsort comparison values:
- Negative value (-1) when first item should come before second
- Zero (0) when items are equal
- Positive value (1) when first item should come after second

## Parameters / Member Variables
- `a`: Pointer to the first ItemPointerData structure to compare
- `b`: Pointer to the second ItemPointerData structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
- Types used:
  - [ItemPointerData](../I/ItemPointerData.md)
  - BlockNumber  
  - OffsetNumber
- Called from:
  - [TidListEval](../T/TidListEval.md) (used with qsort and qunique functions)

## Notes and Other Information
- This is a static function, only accessible within nodeTidscan.c
- Designed specifically for use with qsort() and qunique() functions
- The ordering produced by this comparator optimizes database page access patterns
- Implements lexicographic ordering: (block_number, offset_number)
- Part of the TID scan optimization infrastructure that eliminates duplicates and ensures efficient heap traversal
- The comparison is purely based on physical tuple location, not on tuple content or logical ordering