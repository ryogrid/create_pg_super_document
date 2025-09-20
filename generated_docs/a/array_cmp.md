# array_cmp

## Location
[src/backend/utils/adt/arrayfuncs.c:3973-4145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3973-L4145)

## Overview
Internal comparison function for arrays that provides lexicographic ordering by comparing array elements pairwise and handling dimensionality differences.

## Definition

```c
static int
array_cmp(FunctionCallInfo fcinfo)
```
## Detailed Description
The  function implements a comprehensive comparison algorithm for PostgreSQL arrays. It performs element-by-element comparison using the appropriate comparison function for the array's element type, following lexicographic ordering principles. When arrays have identical elements up to the length of the shorter array, it applies additional rules based on array dimensionality, bounds, and lower bounds to establish a total ordering.

The function handles NULL values by treating two NULLs as equal and considering NULL greater than any non-NULL value. It uses the type cache system to efficiently look up and cache comparison functions for the array element type.

## Parameters / Member Variables
- : Function call information structure containing:
  - Array arguments (accessed via  and )
  - Collation information (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  -  - Get number of dimensions
  -  - Get dimension sizes array
  -  - Get element type OID
  -  - Get lower bounds array
  -  - Calculate total number of items
  -  - Get cached type information
  -  - [Initialize](../I/Initialize.md) array iterator
  -  - Get next array element
  -  - Call element comparison function
  -  - Free detoasted array copies

- Called from (representative examples):
  -  - Array less-than operator
  -  - Array greater-than operator
  -  - Array less-than-or-equal operator
  -  - Array greater-than-or-equal operator
  -  - B-tree comparison support function
  -  - Return larger of two arrays
  -  - Return smaller of two arrays

## Notes and Other Information
- Returns -1 (first array is smaller), 0 (arrays are equal), or 1 (first array is larger)
- Requires arrays to have the same element type; raises error for type mismatches
- Uses cached type information to avoid repeated function lookups during index operations
- Comparison hierarchy: element values → number of items → number of dimensions → dimension sizes → lower bounds
- Handles toasted arrays properly by freeing detoasted copies to prevent memory leaks
- NULL handling follows PostgreSQL's standard semantics where NULL > any non-NULL value