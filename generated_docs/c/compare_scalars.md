# compare_scalars

## Location
[src/backend/commands/analyze.c:2885-2915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L2885-L2915)

## Overview
A comparator function used for sorting ScalarItems during PostgreSQL's ANALYZE operation, which also maintains equality tracking information to optimize statistical computations.

## Definition

```c
static int
compare_scalars(const void *a, const void *b, void *arg)
```
## Detailed Description
The  function serves as a custom comparator for sorting  structures during statistical analysis in PostgreSQL's ANALYZE command. Beyond simple comparison, it performs an important optimization by maintaining a  array that tracks equal datums. When two  elements contain equal datums, the function updates this array to record the relationship, allowing  to avoid redundant comparisons later in the analysis process.

The function uses PostgreSQL's  to perform the actual datum comparison using the appropriate sort operator for the column's data type. For equal datums, it falls back to sorting by tuple number () to ensure deterministic ordering.

## Parameters / Member Variables
- : Pointer to the first  to compare
- : Pointer to the second  to compare  
- : Pointer to  containing comparison context and the  array for tracking equal values

## Dependencies
- Functions called/Symbols referenced:
  -  (struct)
  -  (struct)
  - 
- Called from (representative examples):
  -  (used as qsort comparator)

## Notes and Other Information
This function is specifically designed for use with  and follows the standard comparator function interface. The equality tracking mechanism via  is a performance optimization that reduces the computational complexity of subsequent statistical calculations by avoiding redundant datum comparisons for values already known to be equal.