# circle_distance

## Location
src/backend/utils/adt/geo_ops.c: 5066 - 5081

## Overview
Computes the minimum distance between two circles, returning 0 if the circles overlap or touch.

## Definition


## Detailed Description
This function calculates the distance between the closest points of two circles. It first computes the distance between the circle centers using point_dt, then subtracts the sum of both radii. If the result is negative (indicating overlapping or touching circles), it returns 0.0 instead.

## Parameters / Member Variables
- First circle (PG_GETARG_CIRCLE_P(0)): Input circle structure containing center point and radius
- Second circle (PG_GETARG_CIRCLE_P(1)): Input circle structure containing center point and radius

## Dependencies
- Functions called/Symbols referenced:
  - CIRCLE (type definition)
  - PG_GETARG_CIRCLE_P (parameter extraction macro)
  - point_dt (distance between two points)
  - float8_mi (floating point subtraction)
  - float8_pl (floating point addition)
  - PG_RETURN_FLOAT8 (return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns 0.0 when circles overlap or touch, making it useful for collision detection
- Part of PostgreSQL's geometric data type operations
- Located in src/backend/utils/adt/geo_ops.c:5066-5081