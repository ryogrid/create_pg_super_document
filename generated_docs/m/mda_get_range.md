# mda_get_range

## Location
[src/backend/utils/adt/arrayutils.c:153-166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayutils.c#L153-L166)

## Overview
Computes ranges (sub-array dimensions) for an array slice by calculating the span of each dimension from start to end indices.

## Definition

```c
void
mda_get_range(int n, int *span, const int *st, const int *endp)
```
## Detailed Description
This utility function calculates the dimensions (spans) for each axis of a multidimensional array slice. It iterates through all dimensions and computes the range for each by subtracting the start index from the end index and adding 1. The function assumes that the caller has already validated the slice endpoints to prevent integer overflow.

The function is used internally by PostgreSQL's array slicing operations to determine the size of each dimension in the resulting sub-array.

## Parameters / Member Variables
- `n`: Number of dimensions to process
- `span`: Output array to store the computed ranges for each dimension
- `st`: Array of start indices for each dimension
- `endp`: Array of end indices for each dimension

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls)
- Called from (representative examples):
  - [array_get_slice](../a/array_get_slice.md)
  - [array_set_slice](../a/array_set_slice.md)
  - [array_slice_size](../a/array_slice_size.md)
  - [array_extract_slice](../a/array_extract_slice.md)
  - [array_insert_slice](../a/array_insert_slice.md)

## Notes and Other Information
- The function assumes caller validation of slice endpoints to prevent overflow
- Simple computation: span[i] = endp[i] - st[i] + 1 for each dimension
- Used as a utility function in various array slicing operations throughout PostgreSQL's array handling code
- Located in src/backend/utils/adt/arrayutils.c:153-166

## Simplified Source

```c
void mda_get_range(int n, int *span, const int *st, const int *endp)
{
    // Calculate range for each dimension: span[i] = end[i] - start[i] + 1
    for (int i = 0; i < n; i++)
        span[i] = endp[i] - st[i] + 1;
}
```