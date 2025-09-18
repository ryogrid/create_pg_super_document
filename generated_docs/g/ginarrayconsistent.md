# ginarrayconsistent

## Location
src/backend/access/gin/ginarrayproc.c: 142 - 225

## Overview
This is a PostgreSQL GIN consistent support function that determines whether indexed array data satisfies query conditions for different array search strategies.

## Definition
```c
Datum ginarrayconsistent(PG_FUNCTION_ARGS)
```

## Detailed Description
The `ginarrayconsistent` function serves as a consistent support function for GIN indexes on arrays. It evaluates whether a given set of indexed array elements satisfies the query conditions based on the search strategy. This function is called during index scans to determine which index entries should be considered as potential matches.

The function handles four different strategies with varying consistency requirements:
- **Overlap Strategy**: Returns true if at least one non-null query element is found in the indexed data
- **Contains Strategy**: Returns true only if all query elements (excluding nulls) are found in the indexed data
- **Contained Strategy**: Always returns true but requires recheck, as GIN cannot definitively determine containment
- **Equal Strategy**: Returns true if all query elements are found, but requires recheck for proper null handling

## Parameters / Member Variables
- : Boolean array (bool*) indicating which query elements were found in the index
- : Strategy number (StrategyNumber) specifying the type of array operation being performed
- : Number of query elements (int32) 
- : Output parameter (bool*) indicating whether tuple-level recheck is required
- : Boolean array (bool*) indicating which query elements are null

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_UINT16 (macro for getting strategy number)
  - GinOverlapStrategy, GinContainsStrategy, GinContainedStrategy, GinEqualStrategy (strategy constants)
- Called from:
  - No direct references found (used through GIN operator class infrastructure)

## Notes and Other Information
- Different strategies have different recheck requirements based on what GIN can definitively determine
- Overlap and Contains strategies don't require recheck as GIN can make definitive decisions
- Contained and Equal strategies always require recheck for complete correctness
- Null handling varies by strategy - Contains strategy explicitly excludes nulls, while Equal strategy allows them
- Essential component of PostgreSQL's array GIN operator classes for query evaluation
- The function provides fast index-level filtering before more expensive tuple-level rechecks