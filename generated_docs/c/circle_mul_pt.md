# circle_mul_pt

## Location
src/backend/utils/adt/geo_ops.c: 4999 - 5013

## Overview
Implements the multiplication operator for a circle with a point, performing rotation and scaling transformations on the circle.

## Definition
```c
Datum circle_mul_pt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_mul_pt` function performs a geometric transformation on a circle by multiplying it with a point. This operation applies both rotation and scaling to the circle. The function multiplies the circles center coordinates with the point coordinates using point multiplication, and scales the radius by the magnitude (distance from origin) of the point. This allows for complex geometric transformations where the point acts as both a scaling factor and rotation operator.

## Parameters / Member Variables
- `circle`: Input circle to be transformed (accessed via PG_GETARG_CIRCLE_P(0))
- `point`: Point used for transformation (accessed via PG_GETARG_POINT_P(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P
  - PG_GETARG_POINT_P
  - point_mul_point
  - float8_mul
  - HYPOT
  - PG_RETURN_CIRCLE_P
  - palloc
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function allocates memory for the result circle using palloc()
- The radius scaling is calculated using the hypotenuse (HYPOT) of the point coordinates, representing the distance from origin
- This is part of PostgreSQLs geometric data type operators for circle manipulation
- The transformation combines point multiplication for the center with magnitude-based scaling for the radius