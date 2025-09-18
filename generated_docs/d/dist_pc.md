# dist_pc

## Location
src/backend/utils/adt/geo_ops.c: 5109 - 5126

## Overview
Calculates the minimum distance between a point and a circle, returning 0 if the point is inside the circle.

## Definition


## Detailed Description
This function computes the distance from a point to the closest point on a circle. It calculates the distance from the point to the circle's center, then subtracts the circle's radius. If the result is negative (indicating the point is inside the circle), it returns 0.0 instead.

## Parameters / Member Variables
- Point (PG_GETARG_POINT_P(0)): Input point for distance calculation
- Circle (PG_GETARG_CIRCLE_P(1)): Input circle structure containing center point and radius

## Dependencies
- Functions called/Symbols referenced:
  - Point (type definition)
  - CIRCLE (type definition)
  - PG_GETARG_POINT_P (parameter extraction macro)
  - PG_GETARG_CIRCLE_P (parameter extraction macro)
  - point_dt (distance between two points)
  - float8_mi (floating point subtraction)
  - PG_RETURN_FLOAT8 (return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns 0.0 when the point is inside or on the circle boundary
- Useful for proximity calculations and spatial queries
- Part of PostgreSQL's geometric distance operations
- Located in src/backend/utils/adt/geo_ops.c:5109-5126