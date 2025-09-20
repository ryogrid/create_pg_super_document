# mda_next_tuple

## Location
[src/backend/utils/adt/arrayutils.c:208-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayutils.c#L208-L232)

## Overview
Generates the lexicographically next n-tuple for iterating through multidimensional array coordinates within specified bounds.

## Definition

```c
struct_array_builtin(arr, CSTRINGOID, &elem_values, NULL, n);
```
## Detailed Description
This function implements a counter-like mechanism for multidimensional arrays, advancing coordinates in lexicographic order. It works similar to an odometer, where the rightmost digit increments first, and when it reaches its maximum value (span-1), it wraps to 0 and carries over to the next dimension.

The function modifies the current tuple in-place and returns the dimension that was advanced, or -1 if all possible tuples have been exhausted (indicating the end of iteration).

For example, iterating through a 2x3 array:
- (0,0) → (0,1) → (0,2) → (1,0) → (1,1) → (1,2) → done

The return value indicates which dimension advanced, which can be useful for optimization in some algorithms.

## Parameters / Member Variables
- `n`: Number of dimensions in the array
- `curr`: Current n-tuple coordinates (modified in-place)
- `span`: Maximum values for each dimension (exclusive upper bounds)

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls)
- Called from (representative examples):
  - [array_slice_size](../a/array_slice_size.md)
  - [array_extract_slice](../a/array_extract_slice.md)
  - [array_insert_slice](../a/array_insert_slice.md)

## Notes and Other Information
- Returns -1 when no next tuple exists (end of iteration)
- Returns 0..n-1 indicating which dimension was advanced
- Implements lexicographic ordering for multidimensional iteration
- The function assumes caller has validated dimensions to prevent overflow
- Essential for systematic traversal of array slices in PostgreSQL
- Located in src/backend/utils/adt/arrayutils.c:208-232