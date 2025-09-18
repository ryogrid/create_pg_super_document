# ApplySortComparator

## Location
[src/include/utils/sortsupport.h:200-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/sortsupport.h#L200-L232)

## Overview
ApplySortComparator is an inline function that applies a sort comparator function and returns a 3-way comparison result, handling reverse-sort and NULL ordering semantics properly.

## Definition


## Detailed Description
This function provides a standardized way to compare two PostgreSQL Datum values while handling NULL values and sort direction according to the sort support configuration. It implements the complete comparison logic including:

1. NULL value handling based on the ssup_nulls_first flag
2. Application of the actual comparator function for non-NULL values
3. Result inversion for reverse sorting when ssup_reverse is set

The function returns a standard 3-way comparison result: negative for less-than, zero for equal, and positive for greater-than.

## Parameters / Member Variables
- : The first Datum value to compare
- : Boolean flag indicating whether datum1 is NULL
- : The second Datum value to compare  
- : Boolean flag indicating whether datum2 is NULL
- : SortSupport structure containing comparator function and sort configuration

## Dependencies
- Functions called/Symbols referenced:
  - SortSupport (struct type)
  - INVERT_COMPARE_RESULT (macro)
  - ssup->comparator (function pointer)
- Called from (representative examples):
  - _bt_load (src/backend/access/nbtree/nbtsort.c:1214)
  - [compare_scalars](../c/compare_scalars.md) (src/backend/commands/analyze.c:2894)
  - [heap_compare_slots](../h/heap_compare_slots.md) (src/backend/executor/nodeGatherMerge.c:771)
  - [MJCompare](../M/MJCompare.md) (src/backend/executor/nodeMergejoin.c:420)
  - [comparetup_heap](../c/comparetup_heap.md) (src/backend/utils/sort/tuplesortvariants.c:1093)

## Notes and Other Information
This is a critical utility function used throughout PostgreSQL's sorting infrastructure. It ensures consistent NULL handling and sort direction behavior across all sorting operations. The function is declared as inline for performance reasons since it's called frequently during sort operations. The NULL comparison logic follows SQL standard semantics where NULLs can be ordered either first or last depending on the sort specification.