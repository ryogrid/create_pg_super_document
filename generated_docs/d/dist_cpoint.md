# dist_cpoint

## Location
[src/backend/utils/adt/geo_ops.c:5127-5142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5127-L5142)

## Overview
Calculates the minimum distance from a circle to a point, returning 0 if the point is inside the circle.

## Definition

```c
Datum
dist_cpoint(PG_FUNCTION_ARGS)
```
## Detailed Description
This function computes the distance from a circle to a point, which is functionally equivalent to dist_pc but with reversed parameter order (circle first, then point). It calculates the distance from the point to the circle's center, then subtracts the circle's radius. If the result is negative (indicating the point is inside the circle), it returns 0.0 instead.

## Parameters / Member Variables
- Circle (PG_GETARG_CIRCLE_P(0)): Input circle structure containing center point and radius
- Point (PG_GETARG_POINT_P(1)): Input point for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  - CIRCLE (type definition)
  - [Point](../P/Point.md) (type definition)
  - PG_GETARG_CIRCLE_P (parameter extraction macro)
  - PG_GETARG_POINT_P (parameter extraction macro)
  - [point_dt](../p/point_dt.md) (distance between two points)
  - [float8_mi](../f/float8_mi.md) (floating point subtraction)
  - PG_RETURN_FLOAT8 (return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Functionally equivalent to dist_pc but with reversed parameter order
- Returns 0.0 when the point is inside or on the circle boundary
- Provides alternative syntax for circle-to-point distance calculations
- Part of PostgreSQL's geometric distance operations
- Located in src/backend/utils/adt/geo_ops.c:5127-5142

## Simplified Source

```c
Datum dist_cpoint(PG_FUNCTION_ARGS) {
    CIRCLE *circle = PG_GETARG_CIRCLE_P(0);
    Point *point = PG_GETARG_POINT_P(1);

    // Distance from point to center minus radius
    float8 result = float8_mi(point_dt(point, &circle->center), circle->radius);

    // Return 0 if point is inside circle
    if (result < 0.0)
        result = 0.0;

    PG_RETURN_FLOAT8(result);
}
```