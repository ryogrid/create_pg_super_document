# point_mul

## Location
[src/backend/utils/adt/geo_ops.c:4167-4181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4167-L4181)

## Overview
The point_mul function performs complex multiplication of two Point geometric objects, treating them as complex numbers in a 2D coordinate system.

## Definition

```c
struct(result,
					float8_div(float8_pl(float8_mul(pt1->x, pt2->x),
										 float8_mul(pt1->y, pt2->y)), div),
					float8_div(float8_mi(float8_mul(pt1->y, pt2->x),
										 float8_mul(pt1->x, pt2->y)), div));
```
## Detailed Description
This function implements complex number multiplication for PostgreSQL Point data types. It takes two Point objects as arguments and returns a new Point representing their complex multiplication. The function serves as a PostgreSQL function wrapper around the static helper function point_mul_point, which performs the actual mathematical computation. Complex multiplication treats each point as a complex number where x is the real part and y is the imaginary part, computing (a+bi) * (c+di) = (ac-bd) + (ad+bc)i.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - Argument 0: First Point (p1) - the multiplicand
  - Argument 1: Second Point (p2) - the multiplier

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P (argument extraction)
  - [palloc](palloc.md) (memory allocation)
  - [point_mul_point](point_mul_point.md) (performs the actual multiplication)
  - PG_RETURN_POINT_P (return value packaging)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- The function allocates memory for a new Point result using palloc
- Uses the point_mul_point helper function for the mathematical computation
- Returns a PostgreSQL Datum containing the result Point
- Part of PostgreSQL's geometric data type operations
- Located in src/backend/utils/adt/geo_ops.c at lines 4167-4181

## Simplified Source

```c
Datum
point_mul(PG_FUNCTION_ARGS)
{
    Point *p1 = PG_GETARG_POINT_P(0);
    Point *p2 = PG_GETARG_POINT_P(1);
    Point *result;

    // Allocate memory for result point
    result = (Point *) palloc(sizeof(Point));

    // Perform complex multiplication
    point_mul_point(result, p1, p2);

    PG_RETURN_POINT_P(result);
}
```