# array_unnest_fctx

## Location
src/backend/utils/adt/arrayfuncs.c: 6252 - 6332

## Overview
A local structure type used within the array_unnest function to maintain state across multiple calls when unnesting an array into individual elements using PostgreSQL's Set Returning Function (SRF) framework.

## Definition


## Detailed Description
The array_unnest_fctx structure is a function context data type specifically designed to support the array_unnest() SQL function implementation. It encapsulates all the state information needed to iterate through an array and return its elements one by one across multiple function calls. This structure is allocated in the multi_call_memory_ctx to persist between function calls during the SRF execution cycle.

The structure works in conjunction with PostgreSQL's array iterator mechanism and handles both regular and expanded array formats. It stores element type information (length, alignment, pass-by-value) to properly extract and return individual array elements.

## Parameters / Member Variables
- : Array iterator structure that handles the actual traversal through the array data
- : Index of the next element to be returned (0-based counter)
- : Total number of elements in the array being unnested
- : Length of individual array elements (-1 for variable-length types)
- : Boolean indicating whether elements are passed by value or reference
- : Alignment requirement for array elements ('c', 's', 'i', or 'd')

## Dependencies
- Functions called/Symbols referenced:
  - array_iter_setup
  - array_iter_next
  - ArrayGetNItems
  - AARR_NDIM
  - AARR_DIMS
  - AARR_ELEMTYPE
  - get_typlenbyvalalign
  - VARATT_IS_EXPANDED_HEADER
- Called from (representative examples):
  - array_unnest (src/backend/utils/adt/arrayfuncs.c:6242)

## Notes and Other Information
- This is a typedef struct defined locally within the array_unnest function, not a global type
- The structure is designed to work with PostgreSQL's SRF (Set Returning Function) framework
- Memory for this structure is allocated in multi_call_memory_ctx to ensure proper cleanup
- Supports both regular arrays and expanded array headers for optimal performance
- Element type information is cached to avoid repeated lookups during iteration