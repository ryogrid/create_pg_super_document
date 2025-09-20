# compare_expanded_ranges

## Location
[src/backend/access/brin/brin_minmax_multi.c:858-895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L858-L895)

## Overview
Compares two ExpandedRange structures for sorting purposes, first by minimum value, then by maximum value.

## Definition

```c
static int
compare_expanded_ranges(const void *a, const void *b, void *arg)
```
## Detailed Description
This function serves as a comparison function for sorting ExpandedRange structures, typically used with qsort. The comparison follows a two-level ordering strategy:

1. **Primary comparison**: Compare the minimum values (minval) of both ranges
2. **Secondary comparison**: If minimum values are equal, compare the maximum values (maxval)

The function is designed to handle range overlaps that can occur when merging two Ranges objects during union operations. Although ranges within a single Ranges object are guaranteed not to overlap, when merging multiple Ranges during union operations, overlaps can occur, making the two-level comparison necessary for proper ordering.

The comparison uses the PostgreSQL function call interface (FunctionCall2Coll) with a user-provided comparison function and collation, allowing it to work with any data type that has a comparison operator.

## Parameters / Member Variables
- : Pointer to the first ExpandedRange structure to compare
- : Pointer to the second ExpandedRange structure to compare  
- : Pointer to compare_context structure containing comparison function and collation information

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (used multiple times for comparisons)
  - [DatumGetBool](../D/DatumGetBool.md) (to extract boolean results from comparison functions)
- Called from (representative examples):
  - [sort_expanded_ranges](../s/sort_expanded_ranges.md) (used as comparison function for qsort)

## Notes and Other Information
- Returns standard comparison function values: -1 (a < b), 0 (a == b), 1 (a > b)
- Uses PostgreSQL's function call interface to support any comparable data type
- Handles collation-aware comparisons through the compare_context
- The two-level comparison ensures consistent ordering even when ranges overlap during merge operations
- Designed to be compatible with standard C library sorting functions like qsort