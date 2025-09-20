# array_unnest

## Location
[src/backend/utils/adt/arrayfuncs.c:6242-6251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L6242-L6251)

## Overview
Implements the PostgreSQL UNNEST function for arrays, converting a single array into a set of rows containing each element of the array as a separate value.

## Definition

```c
typedef struct
	{
		array_iter	iter;
		int			nextelem;
		int			numelems;
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
	} array_unnest_fctx;
```
## Detailed Description
The `array_unnest` function is a Set Returning Function (SRF) that takes an array as input and returns each element of the array as a separate row. This function is the backend implementation of PostgreSQL's UNNEST SQL function, which transforms an array into a table-like structure.

The function uses PostgreSQL's SRF framework to maintain state between calls, allowing it to return one array element per function invocation until all elements have been processed. It maintains an internal context structure (`array_unnest_fctx`) to track:
- The current position in the array iteration
- The total number of elements in the array  
- Element type information (length, pass-by-value flag, alignment)

The function handles both regular and expanded array representations efficiently, extracting type information directly from expanded arrays when available to avoid additional catalog lookups.

## Parameters / Member Variables

### Function Parameters:
- Argument 0: The input array of any type (AnyArrayType)

### array_unnest_fctx Structure Members:
- `iter`: Array iterator for traversing array elements
- `nextelem`: Index of the next element to return
- `numelems`: Total number of elements in the array
- `elmlen`: Length of each array element type
- `elmbyval`: Whether elements are passed by value
- `elmalign`: Memory alignment requirement for elements

## Dependencies
- Functions called/Symbols referenced:
  - array_iter_setup
  - array_iter_next
  - ArrayGetNItems
  - AARR_NDIM
  - AARR_DIMS
  - AARR_ELEMTYPE
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - SRF_IS_FIRSTCALL
  - SRF_FIRSTCALL_INIT
  - SRF_PERCALL_SETUP
  - SRF_RETURN_NEXT
  - SRF_RETURN_DONE
  - PG_GETARG_ANY_ARRAY_P
  - VARATT_IS_EXPANDED_HEADER
- Called from (representative examples):
  - SQL UNNEST function calls
  - Array processing operations in query execution

## Notes and Other Information
- This is a Set Returning Function (SRF) that maintains state across multiple calls
- Uses PostgreSQL's multi-call function context for cross-call persistence
- Handles both traditional and expanded array formats for optimal performance
- Memory management ensures detoasted arrays persist for the duration of the function execution
- The function is located in src/backend/utils/adt/arrayfuncs.c:6242-6324
- Part of PostgreSQL's array processing infrastructure and SQL standard compliance