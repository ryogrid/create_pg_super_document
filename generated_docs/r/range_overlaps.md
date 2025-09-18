# range_overlaps

## Location
src/backend/utils/adt/rangetypes.c: 874 - 886

## Overview
The range_overlaps function is a PostgreSQL built-in function that tests whether two range values overlap (have any elements in common).

## Definition


## Detailed Description
This function implements the PostgreSQL range overlaps operator (&& operator). It takes two range values as input and returns a boolean indicating whether they have any overlapping elements. The function serves as a wrapper that extracts the range arguments from the PostgreSQL function call context and delegates the actual overlap checking to the internal range_overlaps_internal function.

The function follows PostgreSQL's standard function interface pattern using the PG_FUNCTION_ARGS mechanism for argument handling and PG_RETURN_BOOL for result return.

## Parameters / Member Variables
- The function uses PostgreSQL's standard function calling convention with implicit parameters accessed via PG_GETARG_RANGE_P macros:
  - First parameter: RangeType pointer to the first range (r1)  
  - Second parameter: RangeType pointer to the second range (r2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P (to extract range arguments)
  - range_get_typcache (to get type cache information)
  - RangeTypeGetOid (to get the OID of the range type)
  - range_overlaps_internal (performs the actual overlap test)
- Called from (representative examples):
  - No direct references found in the codebase (typically called via SQL operator &&)

## Notes and Other Information
- Located in src/backend/utils/adt/rangetypes.c:874-886
- This is the external interface for the range overlaps operation
- The actual overlap logic is implemented in range_overlaps_internal
- Used internally by PostgreSQL when the && operator is applied to range types in SQL queries
- Returns false if either range is empty, as empty ranges do not overlap with anything