# mda_get_prod

## Location
[src/backend/utils/adt/arrayutils.c:167-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayutils.c#L167-L182)

## Overview
Computes the products of array dimensions to calculate scale factors for multidimensional array subscripts.

## Definition


## Detailed Description
This utility function calculates the scale factors (products) needed for converting multidimensional array subscripts into linear offsets. It works by computing cumulative products of dimension sizes from right to left. The rightmost dimension has a scale factor of 1, and each preceding dimension's scale factor is the product of all dimensions to its right.

For example, in a 3D array with dimensions [2][3][4]:
- prod[2] = 1 (rightmost)
- prod[1] = 1 * 4 = 4
- prod[0] = 4 * 3 = 12

This allows converting subscripts (i,j,k) to linear offset: i*prod[0] + j*prod[1] + k*prod[2].

## Parameters / Member Variables
- `n`: Number of dimensions in the array
- `range`: Array containing the size of each dimension
- `prod`: Output array to store the computed scale factors for each dimension

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls)
- Called from (representative examples):
  - [array_slice_size](../a/array_slice_size.md)
  - [array_extract_slice](../a/array_extract_slice.md)
  - [array_insert_slice](../a/array_insert_slice.md)

## Notes and Other Information
- The function assumes caller has validated dimensions to prevent overflow
- Computation starts from the rightmost dimension and works leftward
- Essential for multidimensional array indexing in PostgreSQL's array implementation
- Used in array slicing operations to calculate memory offsets
- Located in src/backend/utils/adt/arrayutils.c:167-182