# dist_ppoly

## Location
src/backend/utils/adt/geo_ops.c: 2612 - 2620

## Overview
PostgreSQL SQL-callable function that calculates the distance from a point to a polygon.

## Definition


## Detailed Description
This function serves as a PostgreSQL SQL-callable wrapper for point-to-polygon distance calculations. It extracts the point and polygon arguments from the PostgreSQL function call interface and delegates the actual computation to the internal  function. This function is fundamental to other distance calculations in the geometric system, as it provides the building block for more complex geometries like circle-to-polygon distance calculations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that contains:
  - Argument 0: Point pointer - the source point geometry
  - Argument 1: POLYGON pointer - the target polygon geometry

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P - extracts Point argument from function call
  - PG_GETARG_POLYGON_P - extracts POLYGON argument from function call
  - dist_ppoly_internal - performs the actual distance calculation
  - PG_RETURN_FLOAT8 - returns float8 result to PostgreSQL
- Called from:
  - No direct references found (likely called via SQL function registry)

## Notes and Other Information
- Located at src/backend/utils/adt/geo_ops.c:2612-2620
- Part of PostgreSQL's geometric data type operations
- Follows PostgreSQL's standard function interface pattern for SQL-callable functions
- The actual distance computation logic is implemented in 
- This function provides the foundation for circle-to-polygon distance calculations, as circles are treated as points plus radius
- The internal function  is also used by  for computing circle-to-polygon distances