# point_div_point

## Location
[src/backend/utils/adt/geo_ops.c:4182-4195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4182-L4195)

## Overview
The point_div_point function performs complex division of two Point objects, implementing complex number division in a 2D coordinate system.

## Definition
static inline void point_div_point(Point *result, Point *pt1, Point *pt2)

## Detailed Description
This static helper function implements complex number division for PostgreSQL Point data types. It divides the first point (pt1) by the second point (pt2), treating them as complex numbers where x represents the real part and y represents the imaginary part. The division follows the formula (a+bi)/(c+di) = ((ac+bd) + (bc-ad)i)/(c²+d²). The function computes the denominator as the sum of squares of pt2's coordinates, then calculates the real and imaginary parts of the quotient.

## Parameters / Member Variables
- result: Pointer to Point where the division result will be stored
- pt1: Pointer to the dividend Point (numerator)
- pt2: Pointer to the divisor Point (denominator)

## Dependencies
- Functions called/Symbols referenced:
  - [float8_pl](../f/float8_pl.md) (floating point addition)
  - [float8_mul](../f/float8_mul.md) (floating point multiplication)
  - [float8_div](../f/float8_div.md) (floating point division)
  - [float8_mi](../f/float8_mi.md) (floating point subtraction)
  - [point_construct](point_construct.md) (constructs the result point)
- Called from (representative examples):
  - [point_div](point_div.md)
  - [box_div](../b/box_div.md)
  - [path_div_pt](path_div_pt.md)
  - [circle_div_pt](../c/circle_div_pt.md)

## Notes and Other Information
- This is a static inline function for internal use within geo_ops.c
- No division by zero checking is performed at this level
- The function assumes valid input pointers
- Used extensively by other geometric operations requiring point division
- Located in src/backend/utils/adt/geo_ops.c at lines 4182-4195

## Simplified Source

```c
static inline void
point_div_point(Point *result, Point *pt1, Point *pt2)
{
    float8 div;

    // Calculate denominator: c² + d² (magnitude squared of pt2)
    div = float8_pl(float8_mul(pt2->x, pt2->x), float8_mul(pt2->y, pt2->y));

    // Complex division: (a+bi)/(c+di) = ((ac+bd) + (bc-ad)i)/(c²+d²)
    point_construct(result,
                    // Real part: (ac + bd) / (c² + d²)
                    float8_div(float8_pl(float8_mul(pt1->x, pt2->x),
                                         float8_mul(pt1->y, pt2->y)), div),
                    // Imaginary part: (bc - ad) / (c² + d²)
                    float8_div(float8_mi(float8_mul(pt1->y, pt2->x),
                                         float8_mul(pt1->x, pt2->y)), div));
}
```