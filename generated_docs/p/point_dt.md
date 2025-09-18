# point_dt

## Location
[src/backend/utils/adt/geo_ops.c:2002-2007](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2002-L2007)

## Overview
Calculates the Euclidean distance between two points in 2D space using the standard distance formula.

## Definition


## Detailed Description
The  function computes the straight-line distance between two points in a 2D coordinate system. It implements the Euclidean distance formula: distance = √((x₂-x₁)² + (y₂-y₁)²). The function uses the  macro which provides a numerically stable implementation of the hypotenuse calculation, avoiding potential overflow issues that could occur with direct squaring and square root operations.

This is a fundamental geometric operation used throughout PostgreSQL's geometric data type system for distance calculations, spatial comparisons, and geometric containment tests.

## Parameters / Member Variables
- : Pointer to the first Point structure containing x and y coordinates
- : Pointer to the second Point structure containing x and y coordinates

## Dependencies
- Functions called/Symbols referenced:
  -  (geometric data type structure)
  -  (floating-point subtraction function)
  -  (hypotenuse calculation macro)
- Called from (representative examples):
  -  (public distance function for points)
  -  (line segment length calculation)
  -  (distance between boxes)
  -  (path length calculation)
  -  (distance between circles)
  -  (point-in-line-segment tests)
  - Various geometric comparison functions (, , , )

## Notes and Other Information
- This is a static inline function, meaning it's optimized for performance and only accessible within the geo_ops.c compilation unit
- The function is heavily used throughout PostgreSQL's geometric operations as a building block for more complex spatial calculations
- Uses PostgreSQL's internal floating-point arithmetic functions to ensure consistent behavior across platforms
- The HYPOT macro provides better numerical stability than naive sqrt(x²+y²) implementations