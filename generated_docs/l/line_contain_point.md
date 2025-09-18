# line_contain_point

## Location
[src/backend/utils/adt/geo_ops.c:3087-3094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3087-L3094)

## Overview
Determines whether a given point lies on a specified line by checking if the point satisfies the line equation.

## Definition


## Detailed Description
This is a static utility function that implements geometric containment testing for points on lines. It uses the standard line equation Ax + By + C = 0 to determine if a point (x, y) lies on the line. The function evaluates the line equation by substituting the point's coordinates and checking if the result is approximately zero using floating-point zero comparison.

The function performs the calculation: A*x + B*y + C, where A, B, and C are the line coefficients stored in the LINE structure, and x, y are the point coordinates. If this expression equals zero (within floating-point tolerance), the point lies on the line.

## Parameters / Member Variables
- : Pointer to a LINE structure containing the coefficients A, B, and C of the line equation Ax + By + C = 0
- : Pointer to a Point structure containing the x and y coordinates to test

## Dependencies
- Functions called/Symbols referenced:
  - [float8_mul](../f/float8_mul.md) (multiplies two float8 values)
  - [float8_pl](../f/float8_pl.md) (adds two float8 values)
  - FPzero (checks if a floating-point value is approximately zero)
- Data types used:
  - LINE (line representation with A, B, C coefficients)
  - [Point](../P/Point.md) (point representation with x, y coordinates)
- Called from (representative examples):
  - [on_pl](../o/on_pl.md) (point on line test)
  - [on_sl](../o/on_sl.md) (point on line segment test)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the geo_ops.c file
- Uses floating-point arithmetic with appropriate tolerance checking via FPzero
- Part of PostgreSQL's geometric data type operations infrastructure
- The function assumes the LINE structure follows the standard mathematical representation Ax + By + C = 0
- Critical for geometric containment queries in PostgreSQL's geometric types system