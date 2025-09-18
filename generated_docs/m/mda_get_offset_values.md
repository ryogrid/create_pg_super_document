# mda_get_offset_values

## Location
src/backend/utils/adt/arrayutils.c: 183 - 207

## Overview
Computes offset distances needed to step through a multidimensional sub-array within the parent array based on dimension products and sub-array spans.

## Definition


## Detailed Description
This utility function calculates the step distances required to navigate through a multidimensional sub-array when it is embedded within a larger parent array. The function computes how much to advance the linear offset when moving from one "row" to the next in each dimension of the sub-array.

The calculation accounts for the difference between the full array dimensions and the sub-array dimensions. For each dimension j, it computes:
- Start with prod[j] - 1 (the distance to skip to next "row" in full array)
- Subtract the portions already covered by inner dimensions of the sub-array

This enables efficient traversal of sub-arrays without having to recompute offsets during iteration.

## Parameters / Member Variables
- `n`: Number of dimensions
- `dist`: Output array to store computed offset distances for each dimension
- `prod`: Array of dimension products (scale factors) from mda_get_prod
- `span`: Array of sub-array spans for each dimension

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls)
- Called from (representative examples):
  - [array_slice_size](../a/array_slice_size.md)
  - [array_extract_slice](../a/array_extract_slice.md)
  - [array_insert_slice](../a/array_insert_slice.md)

## Notes and Other Information
- The function assumes caller has validated dimensions to prevent overflow
- Works in conjunction with mda_get_prod to enable efficient sub-array traversal
- The rightmost dimension always has dist[n-1] = 0 since no stepping is needed
- Essential for implementing array slicing operations in PostgreSQL
- Located in src/backend/utils/adt/arrayutils.c:183-207