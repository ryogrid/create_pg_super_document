# interval_cmp_lower

## Location
[src/backend/utils/adt/rangetypes_gist.c:1744-1756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1744-L1756)

## Overview
A comparison function that compares two SplitInterval structures based on their lower bound values for use in GiST index operations.

## Definition

```c
static int
interval_cmp_lower(const void *a, const void *b, void *arg)
```
## Detailed Description
This function serves as a comparator for sorting SplitInterval structures during GiST (Generalized Search Tree) index splitting operations. It extracts the lower bound values from two SplitInterval structures and compares them using PostgreSQL's internal float8 comparison logic.

The function follows the standard qsort comparator interface, returning a negative value if the first interval's lower bound is smaller, zero if they are equal, and a positive value if the first is larger. This enables efficient sorting of intervals based on their starting points.

## Parameters / Member Variables
- : Pointer to the first SplitInterval structure to compare
- : Pointer to the second SplitInterval structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - SplitInterval (struct type)
  - [float8_cmp_internal](../f/float8_cmp_internal.md)
- Called from:
  - [gist_box_picksplit](../g/gist_box_picksplit.md) (src/backend/access/gist/gistproc.c:567)
  - rangeCopy (src/backend/utils/adt/rangetypes_gist.c:182)
  - [range_gist_double_sorting_split](../r/range_gist_double_sorting_split.md) (src/backend/utils/adt/rangetypes_gist.c:1368)

## Notes and Other Information
- This is a static function used internally within GiST indexing operations
- Designed specifically for use with qsort or similar sorting algorithms
- Part of the GiST splitting strategy to maintain balanced tree structures by organizing intervals based on their lower bounds
- The comparison uses float8 data type, indicating this works with floating-point interval bounds
- Used in both box/rectangle indexing and range type indexing contexts