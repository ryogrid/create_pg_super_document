# dist_bs

## Location
[src/backend/utils/adt/geo_ops.c:2562-2570](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2562-L2570)

## Overview
Calculates the distance from a box geometric object to a line segment (lseg).

## Definition

```c
Datum
dist_bs(PG_FUNCTION_ARGS)
```
## Detailed Description
This function computes the shortest distance between a box and a line segment in PostgreSQL's geometric data type system. It serves as a PostgreSQL SQL-callable function wrapper that extracts the input arguments and delegates the actual distance calculation to the internal  function. The function returns the computed distance as a floating-point value.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that contains:
  - Argument 0: BOX pointer - the source box geometry
  - Argument 1: LSEG pointer - the target line segment geometry

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P - extracts BOX argument from function call
  - PG_GETARG_LSEG_P - extracts LSEG argument from function call  
  - [box_closept_lseg](../b/box_closept_lseg.md) - performs the actual distance calculation
  - PG_RETURN_FLOAT8 - returns float8 result to PostgreSQL
- Called from:
  - No direct references found (likely called via SQL function registry)

## Notes and Other Information
- Located at src/backend/utils/adt/geo_ops.c:2562-2570
- Part of PostgreSQL's geometric data type operations
- Follows PostgreSQL's standard function interface pattern for SQL-callable functions
- The actual distance computation logic is implemented in 