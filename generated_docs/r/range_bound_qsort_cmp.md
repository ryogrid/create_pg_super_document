# range_bound_qsort_cmp

## Location
[src/backend/utils/adt/rangetypes_typanalyze.c:112-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_typanalyze.c#L112-L124)

## Overview
The  function is a comparison function for sorting RangeBound structures, used during range statistics computation to order range boundaries for analysis.

## Definition

```c
static int
range_bound_qsort_cmp(const void *a1, const void *a2, void *arg)
```
## Detailed Description
This function serves as an adapter between the standard qsort interface and PostgreSQL's internal range boundary comparison logic. It wraps the  function to provide a qsort-compatible interface for sorting arrays of RangeBound structures.

The function enables sorting of range boundaries according to their natural ordering, taking into account both the boundary values and their inclusive/exclusive nature. This is essential for range statistics computation where boundaries need to be processed in sorted order.

## Parameters / Member Variables
- : Pointer to the first RangeBound structure to compare
- : Pointer to the second RangeBound structure to compare  
- : Pointer to TypeCacheEntry containing type-specific comparison information

## Dependencies
- Functions called/Symbols referenced:
  -  (structure representing a range boundary)
  -  (core function for comparing range boundaries)
- Called from:
  -  (used twice for sorting lower and upper bounds separately)

## Notes and Other Information
- The function is declared static, making it internal to the rangetypes_typanalyze.c file
- Relies on the TypeCacheEntry argument to provide type-specific comparison functions for the boundary values
- Returns the same values as : negative, zero, or positive integers indicating comparison result
- Used specifically in range statistics computation to sort boundary arrays for efficient processing
- Handles the complexity of range boundary comparison including boundary inclusivity/exclusivity semantics
- Essential for creating histograms and other statistical summaries of range data