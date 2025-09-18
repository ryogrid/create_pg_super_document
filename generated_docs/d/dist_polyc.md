# dist_polyc

## Location
src/backend/utils/adt/geo_ops.c: 2600 - 2611

## Overview
PostgreSQL SQL-callable function that calculates the distance from a polygon to a circle.

## Definition


## Detailed Description
This function serves as a PostgreSQL SQL-callable wrapper for polygon-to-circle distance calculations. It extracts the polygon and circle arguments from the PostgreSQL function call interface and delegates the actual computation to the internal  function. Note that despite the different parameter order (polygon first, then circle), it uses the same internal implementation as  since distance calculations are symmetric. The function follows PostgreSQL's standard pattern for geometric operation functions.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that contains:
  - Argument 0: POLYGON pointer - the source polygon geometry
  - Argument 1: CIRCLE pointer - the target circle geometry

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P - extracts POLYGON argument from function call
  - PG_GETARG_CIRCLE_P - extracts CIRCLE argument from function call
  - [dist_cpoly_internal](dist_cpoly_internal.md) - performs the actual distance calculation
  - PG_RETURN_FLOAT8 - returns float8 result to PostgreSQL
- Called from:
  - No direct references found (likely called via SQL function registry)

## Notes and Other Information
- Located at src/backend/utils/adt/geo_ops.c:2600-2611
- Part of PostgreSQL's geometric data type operations
- Follows PostgreSQL's standard function interface pattern for SQL-callable functions
- The actual distance computation logic is implemented in 
- Complementary to  which calculates circle-to-polygon distance using the same internal function
- Despite different parameter order, both functions use the same internal implementation due to the symmetric nature of distance calculations