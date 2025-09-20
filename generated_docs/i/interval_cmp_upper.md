# interval_cmp_upper

## Location
[src/backend/utils/adt/rangetypes_gist.c:1757-1769](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1757-L1769)

## Overview
A comparison function that compares two SplitInterval structures based on their upper bound values for use in GiST index operations.

## Definition

```c
static int
interval_cmp_upper(const void *a, const void *b, void *arg)
```
## Detailed Description
This function serves as a comparator for sorting SplitInterval structures during GiST (Generalized Search Tree) index splitting operations. It extracts the upper bound values from two SplitInterval structures and compares them using PostgreSQL's internal float8 comparison logic.

The function follows the standard qsort comparator interface, returning a negative value if the first interval's upper bound is smaller, zero if they are equal, and a positive value if the first is larger. This enables efficient sorting of intervals based on their ending points, which is complementary to the lower bound sorting provided by interval_cmp_lower.

## Parameters / Member Variables
- : Pointer to the first SplitInterval structure to compare
- : Pointer to the second SplitInterval structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - SplitInterval (struct type)
  - [float8_cmp_internal](../f/float8_cmp_internal.md)
- Called from:
  - [gist_box_picksplit](../g/gist_box_picksplit.md) (src/backend/access/gist/gistproc.c:569)
  - rangeCopy (src/backend/utils/adt/rangetypes_gist.c:183)
  - [range_gist_double_sorting_split](../r/range_gist_double_sorting_split.md) (src/backend/utils/adt/rangetypes_gist.c:1370)

## Notes and Other Information
- This is a static function used internally within GiST indexing operations
- Designed specifically for use with qsort or similar sorting algorithms
- Part of the GiST splitting strategy to maintain balanced tree structures by organizing intervals based on their upper bounds
- The comparison uses float8 data type, indicating this works with floating-point interval bounds
- Used in conjunction with interval_cmp_lower to provide different sorting strategies during index node splitting
- Applied in both box/rectangle indexing and range type indexing contexts