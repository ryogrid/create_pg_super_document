# dist_cpoly

## Location
[src/backend/utils/adt/geo_ops.c:2588-2599](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2588-L2599)

## Overview
PostgreSQL SQL-callable function that calculates the distance from a circle to a polygon.

## Definition

```c
Datum
dist_cpoly(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL SQL-callable wrapper for circle-to-polygon distance calculations. It extracts the circle and polygon arguments from the PostgreSQL function call interface and delegates the actual computation to the internal  function. The function follows PostgreSQL's standard pattern for geometric operation functions, handling argument extraction and return value formatting while keeping the core logic in a separate internal function.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that contains:
  - Argument 0: CIRCLE pointer - the source circle geometry
  - Argument 1: POLYGON pointer - the target polygon geometry

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P - extracts CIRCLE argument from function call
  - PG_GETARG_POLYGON_P - extracts POLYGON argument from function call
  - [dist_cpoly_internal](dist_cpoly_internal.md) - performs the actual distance calculation
  - PG_RETURN_FLOAT8 - returns float8 result to PostgreSQL
- Called from:
  - No direct references found (likely called via SQL function registry)

## Notes and Other Information
- Located at src/backend/utils/adt/geo_ops.c:2588-2599
- Part of PostgreSQL's geometric data type operations
- Follows PostgreSQL's standard function interface pattern for SQL-callable functions
- The actual distance computation logic is implemented in 
- Complementary to  which calculates polygon-to-circle distance using the same internal function

## Simplified Source

```c
Datum
dist_cpoly(PG_FUNCTION_ARGS)
{
    // Extract circle and polygon arguments
    CIRCLE *circle = PG_GETARG_CIRCLE_P(0);
    POLYGON *poly = PG_GETARG_POLYGON_P(1);

    // Calculate distance using internal function and return result
    PG_RETURN_FLOAT8(dist_cpoly_internal(circle, poly));
}
```