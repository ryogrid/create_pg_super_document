# ginqueryarrayextract

## Location
src/backend/access/gin/ginarrayproc.c: 79 - 141

## Overview
This is a PostgreSQL GIN extractQuery support function that processes query arrays for different search strategies, determining how array elements should be matched during index scans.

## Definition
```c
Datum ginqueryarrayextract(PG_FUNCTION_ARGS)
```

## Detailed Description
The `ginqueryarrayextract` function serves as an extractQuery support function for GIN indexes on arrays. It extracts query elements from an input array and sets the appropriate search mode based on the query strategy. This function is crucial for translating high-level array operations (like overlap, contains, contained by, and equality) into the specific search behaviors that the GIN index can execute efficiently.

The function handles four main search strategies:
- **Overlap**: Default search mode for finding arrays that share any elements
- **Contains**: Default search for non-empty queries, special "all" mode for empty arrays
- **Contained by**: Always uses include-empty mode since empty sets are contained in everything  
- **Equality**: Uses default mode for non-empty arrays, include-empty mode for empty arrays

## Parameters / Member Variables
- : Input query array (PG_GETARG_ARRAYTYPE_P_COPY(0)) - the array representing the query condition
- : Output parameter (int32*) - returns the number of extracted query elements
- : Query strategy number (StrategyNumber) - specifies the type of array operation
- : Output parameter (bool**) - returns array of null flags for each query element  
- : Output parameter (int32*) - returns the search mode for the GIN scan

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P_COPY (macro for getting array argument)
  - PG_GETARG_UINT16 (macro for getting strategy number)
  - [get_typlenbyvalalign](get_typlenbyvalalign.md) (gets type information for array elements)
  - ARR_ELEMTYPE (macro to get array element type)
  - [deconstruct_array](../d/deconstruct_array.md) (decomposes array into elements and null flags)
  - GinOverlapStrategy, GinContainsStrategy, GinContainedStrategy, GinEqualStrategy (strategy constants)
  - GIN_SEARCH_MODE_DEFAULT, GIN_SEARCH_MODE_ALL, GIN_SEARCH_MODE_INCLUDE_EMPTY (search mode constants)
- Called from:
  - No direct references found (used through operator class infrastructure)

## Notes and Other Information  
- Different strategies require different search modes to handle empty arrays correctly
- Contains strategy: empty arrays contain everything, so uses GIN_SEARCH_MODE_ALL
- Contained strategy: empty arrays are contained in everything, so uses GIN_SEARCH_MODE_INCLUDE_EMPTY
- Memory management mirrors ginarrayextract - array copy is retained as elements point into it
- Essential component of PostgreSQL's array GIN operator classes for query processing
- The function includes comprehensive error handling for unknown strategies