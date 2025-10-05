# line_interpt_line

## Location
[src/backend/utils/adt/geo_ops.c:1314-1379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1314-L1379)

## Overview
Internal function that computes the intersection point of two lines using mathematical formulas and returns whether the lines intersect.

## Definition

```c
struct(result, x, y);
```
## Detailed Description
This is the core implementation function for line intersection calculations in PostgreSQL's geometric operations. It uses the mathematical formulas for line intersection based on the standard line equation Ax + By + C = 0. The function handles various edge cases including parallel lines, identical lines, and lines with NaN constants. It returns true if the lines intersect and false if they are parallel. When lines intersect, it calculates the intersection coordinates and stores them in the result Point if provided.

The algorithm works by:
1. Checking if l1->B is non-zero and using it as the primary calculation path
2. Falling back to l2->B if l1->B is zero but l2->B is non-zero
3. Returning false if both B coefficients are zero (both lines are vertical)
4. Using cross-multiplication to solve the system of linear equations
5. Handling floating-point precision issues by normalizing -0.0 to 0.0

## Parameters / Member Variables
- : Pointer to Point where intersection coordinates will be stored (can be NULL for intersection test only)
- : First LINE object with coefficients A, B, C for equation Ax + By + C = 0
- : Second LINE object with coefficients A, B, C for equation Ax + By + C = 0

## Dependencies
- Functions called/Symbols referenced:
  - FPzero (floating-point zero comparison)
  - [FPeq](../F/FPeq.md) (floating-point equality comparison)
  - [float8_mul](../f/float8_mul.md), float8_div, float8_mi, float8_pl (floating-point arithmetic)
  - [point_construct](../p/point_construct.md) (constructs Point from coordinates)
- Types used:
  - [Point](../P/Point.md) (geometric point type)
  - LINE (geometric line type)
  - float8 (double precision floating-point)
- Called from:
  - [line_interpt](line_interpt.md) (main SQL function wrapper)
  - [line_intersect](line_intersect.md) (line intersection test)
  - [line_parallel](line_parallel.md) (parallel line test)
  - [line_distance](line_distance.md) (distance between lines)
  - [lseg_interpt_line](lseg_interpt_line.md) (line segment to line intersection)
  - [line_closept_point](line_closept_point.md) (closest point on line to point)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1314-1379
- Internal static function, not directly accessible from SQL
- Handles the special case where identical lines are considered parallel (no unique intersection)
- Returns true for lines with NaN constants, producing NaN intersection coordinates
- Includes floating-point precision handling to normalize -0.0 to 0.0
- Core mathematical implementation used by multiple geometric functions in PostgreSQL

## Simplified Source

```c
static bool line_interpt_line(Point *result, LINE *l1, LINE *l2) {
    float8 x, y;

    // Use l1->B as primary calculation path
    if (!FPzero(l1->B)) {
        // Check if lines are parallel: l2->A == l1->A * (l2->B / l1->B)
        if (FPeq(l2->A, float8_mul(l1->A, float8_div(l2->B, l1->B))))
            return false;

        // Calculate intersection coordinates using cross-multiplication
        x = float8_div(float8_mi(float8_mul(l1->B, l2->C), float8_mul(l2->B, l1->C)),
                       float8_mi(float8_mul(l1->A, l2->B), float8_mul(l2->A, l1->B)));
        y = float8_div(-float8_pl(float8_mul(l1->A, x), l1->C), l1->B);
    }
    // Fallback to l2->B if l1->B is zero
    else if (!FPzero(l2->B)) {
        // Check parallel condition with roles reversed
        if (FPeq(l1->A, float8_mul(l2->A, float8_div(l1->B, l2->B))))
            return false;

        // Calculate intersection with l2 as reference
        x = float8_div(float8_mi(float8_mul(l2->B, l1->C), float8_mul(l1->B, l2->C)),
                       float8_mi(float8_mul(l2->A, l1->B), float8_mul(l1->A, l2->B)));
        y = float8_div(-float8_pl(float8_mul(l2->A, x), l2->C), l2->B);
    }
    // Both lines are vertical (B=0), no intersection
    else
        return false;

    // Normalize -0.0 to 0.0 for platform consistency
    if (x == 0.0) x = 0.0;
    if (y == 0.0) y = 0.0;

    // Store result if requested
    if (result != NULL)
        point_construct(result, x, y);

    return true;
}
```