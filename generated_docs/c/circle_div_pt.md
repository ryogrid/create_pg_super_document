# circle_div_pt

## Location
src/backend/utils/adt/geo_ops.c: 5014 - 5031

## Overview
Implements the division operator for a circle with a point, performing inverse rotation and scaling transformations on the circle.

## Definition
```c
Datum circle_div_pt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_div_pt` function performs the inverse geometric transformation of `circle_mul_pt` by dividing a circle by a point. This operation applies inverse rotation and scaling to the circle. The function divides the circles center coordinates by the point coordinates using point division, and scales the radius down by the magnitude (distance from origin) of the point. This allows for inverse geometric transformations where the point acts as both an inverse scaling factor and inverse rotation operator.

## Parameters / Member Variables
- `circle`: Input circle to be transformed (accessed via PG_GETARG_CIRCLE_P(0))
- `point`: Point used for inverse transformation (accessed via PG_GETARG_POINT_P(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P
  - PG_GETARG_POINT_P
  - [point_div_point](../p/point_div_point.md)
  - [float8_div](../f/float8_div.md)
  - HYPOT
  - PG_RETURN_CIRCLE_P
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function allocates memory for the result circle using palloc()
- The radius scaling uses division by the hypotenuse (HYPOT) of the point coordinates
- This is the inverse operation of circle_mul_pt
- Part of PostgreSQLs geometric data type operators for circle manipulation
- The transformation combines point division for the center with magnitude-based inverse scaling for the radius