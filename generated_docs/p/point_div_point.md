# point_div_point

## Location
src/backend/utils/adt/geo_ops.c: 4182 - 4195

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
  - float8_pl (floating point addition)
  - float8_mul (floating point multiplication)
  - float8_div (floating point division)
  - float8_mi (floating point subtraction)
  - point_construct (constructs the result point)
- Called from (representative examples):
  - point_div
  - box_div
  - path_div_pt
  - circle_div_pt

## Notes and Other Information
- This is a static inline function for internal use within geo_ops.c
- No division by zero checking is performed at this level
- The function assumes valid input pointers
- Used extensively by other geometric operations requiring point division
- Located in src/backend/utils/adt/geo_ops.c at lines 4182-4195