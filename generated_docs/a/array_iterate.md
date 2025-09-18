# array_iterate

## Location
[src/backend/utils/adt/arrayfuncs.c:4664-4746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4664-L4746)

## Overview
This function advances an ArrayIterator to the next element or slice, returning the corresponding data and indicating whether more elements remain.

## Definition


## Detailed Description
The  function is the core iteration mechanism for traversing PostgreSQL arrays. It operates in two modes based on the iterator's configuration: scalar mode (returning individual elements) and slice mode (returning sub-arrays). In scalar mode, it extracts single elements, properly handling NULL values and advancing the data pointer. In slice mode, it constructs entire sub-arrays by collecting elements for the specified slice dimensions. The function returns true while elements remain and false when iteration is complete.

## Parameters / Member Variables
- : The ArrayIterator object containing iteration state and configuration
- : Output parameter to store the returned Datum (element or sub-array)
- : Output parameter indicating whether the returned value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if array element at given position is NULL
  - : Extracts attribute value from data pointer with proper type handling
  - : Advances pointer by attribute length
  - : Aligns pointer to proper boundary for data type
  - : Constructs multi-dimensional array from values and nulls
  - : Gets element type of the array
  - : Converts pointer to Datum
- Called from (representative examples):
  - : For finding element positions in arrays
  - : For finding all positions of elements in arrays

## Notes and Other Information
- Returns false when iteration reaches the end (current_item >= nitems)
- In scalar mode (slice_ndim == 0), returns individual elements with proper NULL handling
- In slice mode (slice_ndim > 0), builds sub-arrays using the pre-allocated workspace
- Automatically advances internal pointers and counters for subsequent iterations
- Handles both fixed-length and variable-length data types correctly
- For slice iteration, constructs proper multi-dimensional arrays with correct dimensions and bounds
- Memory management for returned arrays in slice mode is handled by the caller