# array_create_iterator

## Location
src/backend/utils/adt/arrayfuncs.c: 4585 - 4663

## Overview
This function creates and initializes an ArrayIterator structure for efficiently traversing through PostgreSQL arrays, supporting both element-by-element and slice-based iteration modes.

## Definition


## Detailed Description
The  function initializes an iterator object for traversing arrays in PostgreSQL. It supports two iteration modes: element-by-element (when slice_ndim is 0) where individual elements are returned, and slice-based iteration (when slice_ndim > 0) where sub-arrays of the rightmost N dimensions are returned. The function allocates and configures all necessary data structures including workspace for building sub-arrays when operating in slice mode. It also handles type information either from the provided ArrayMetaState or by looking it up from the system catalogs.

## Parameters / Member Variables
- : Pointer to the PostgreSQL array to iterate over (must remain valid for iterator lifetime)
- : Number of dimensions for slicing (0 for element iteration, 1-ARR_NDIM for slice iteration)
- : Optional pre-computed array metadata state containing type information (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - : Allocates and zeroes memory for the iterator structure
  - : Validates input pointer
  - : Gets number of array dimensions
  - : Gets null bitmap from array
  - : Calculates total number of items in array dimensions
  - : Gets array element type
  - : Retrieves type length, by-value flag, and alignment
  - : Gets array dimension information
  - : Gets array lower bounds
  - : Gets pointer to array data
- Called from (representative examples):
  - : For finding element positions in arrays
  - : For finding all positions of elements in arrays

## Notes and Other Information
- The iterator must be freed using  to prevent memory leaks
- When slice_ndim > 0, the function allocates workspace arrays ( and ) for constructing sub-arrays
- The passed array must remain valid for the entire lifetime of the iterator
- Performs sanity checks on slice_ndim parameter to ensure it's within valid bounds
- Supports both cases where ArrayMetaState is pre-computed (for efficiency) or looked up dynamically
- Sets up all necessary pointers and counters for subsequent iteration via 