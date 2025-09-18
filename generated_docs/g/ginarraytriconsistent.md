# ginarraytriconsistent

## Location
[src/backend/access/gin/ginarrayproc.c:226-305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginarrayproc.c#L226-L305)

## Overview
This is a PostgreSQL GIN triconsistent support function that provides three-valued logic (true/false/maybe) for determining whether indexed array data satisfies query conditions.

## Definition
```c
Datum ginarraytriconsistent(PG_FUNCTION_ARGS)
```

## Detailed Description
The `ginarraytriconsistent` function serves as an enhanced consistent support function for GIN indexes on arrays, utilizing three-valued logic (GIN_TRUE, GIN_FALSE, GIN_MAYBE) to provide more precise index filtering. Unlike the binary consistent function, triconsistent can express uncertainty when the index alone cannot definitively determine whether a condition is met, allowing for more efficient query processing by avoiding unnecessary tuple fetches.

The function evaluates different array search strategies with ternary logic:
- **Overlap Strategy**: Returns GIN_TRUE if any non-null element is confirmed present, GIN_MAYBE if any are uncertain, GIN_FALSE otherwise
- **Contains Strategy**: Returns GIN_TRUE only if all elements are confirmed present, GIN_FALSE if any are absent, GIN_MAYBE if any are uncertain
- **Contained Strategy**: Always returns GIN_MAYBE as the index cannot definitively determine containment relationships
- **Equal Strategy**: Returns GIN_FALSE if any element is definitely absent, otherwise GIN_MAYBE

## Parameters / Member Variables
- : Ternary value array (GinTernaryValue*) indicating the status of each query element (TRUE/FALSE/MAYBE)
- : Strategy number (StrategyNumber) specifying the type of array operation
- : Number of query elements (int32)
- : Boolean array (bool*) indicating which query elements are null

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_UINT16 (macro for getting strategy number)
  - GinTernaryValue type and constants (GIN_TRUE, GIN_FALSE, GIN_MAYBE)
  - GinOverlapStrategy, GinContainsStrategy, GinContainedStrategy, GinEqualStrategy (strategy constants)
  - PG_RETURN_GIN_TERNARY_VALUE (macro for returning ternary result)
- Called from:
  - No direct references found (used through GIN operator class infrastructure)

## Notes and Other Information
- Provides more efficient index scans than binary consistent function by reducing false positives
- Three-valued logic allows GIN to skip tuple fetches when index information is sufficient  
- Different strategies have varying degrees of certainty that can be expressed through the index
- Null element handling varies by strategy, similar to the binary consistent function
- Part of PostgreSQL's advanced GIN indexing infrastructure for optimized array query processing
- Enhanced version of ginarrayconsistent that provides better selectivity estimation