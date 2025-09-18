# dist_cpoint

## Location
src/backend/utils/adt/geo_ops.c: 5127 - 5142

## Overview
Calculates the minimum distance from a circle to a point, returning 0 if the point is inside the circle.

## Definition


## Detailed Description
This function computes the distance from a circle to a point, which is functionally equivalent to dist_pc but with reversed parameter order (circle first, then point). It calculates the distance from the point to the circle's center, then subtracts the circle's radius. If the result is negative (indicating the point is inside the circle), it returns 0.0 instead.

## Parameters / Member Variables
- Circle (PG_GETARG_CIRCLE_P(0)): Input circle structure containing center point and radius
- Point (PG_GETARG_POINT_P(1)): Input point for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  - CIRCLE (type definition)
  - Point (type definition)
  - PG_GETARG_CIRCLE_P (parameter extraction macro)
  - PG_GETARG_POINT_P (parameter extraction macro)
  - point_dt (distance between two points)
  - float8_mi (floating point subtraction)
  - PG_RETURN_FLOAT8 (return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Functionally equivalent to dist_pc but with reversed parameter order
- Returns 0.0 when the point is inside or on the circle boundary
- Provides alternative syntax for circle-to-point distance calculations
- Part of PostgreSQL's geometric distance operations
- Located in src/backend/utils/adt/geo_ops.c:5127-5142