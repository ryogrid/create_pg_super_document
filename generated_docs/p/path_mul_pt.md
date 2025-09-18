# path_mul_pt

## Location
src/backend/utils/adt/geo_ops.c: 4425 - 4437

## Overview
Applies rotation and scaling transformation to a path by multiplying each point with a transformation point, enabling complex geometric transformations.

## Definition
```c
Datum path_mul_pt(PG_FUNCTION_ARGS)  
```

## Detailed Description
The `path_mul_pt` function performs rotation and scaling operations on a PATH by multiplying each point in the path with a transformation Point. This operation treats the transformation point as a complex number, enabling combined scaling and rotation transformations in a single operation. The function operates in-place on a copy of the input path, applying the same transformation to all points uniformly.

This is one of PostgreSQL's geometric transformation operators, allowing complex transformations that can scale, rotate, or combine both operations depending on the values in the transformation point. The multiplication operation follows complex number arithmetic where points are treated as complex numbers (x + yi).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `path`: The PATH to be transformed (copied for modification)
  - `point`: The Point representing the transformation parameters for scaling/rotation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P_COPY (macro for retrieving and copying PATH argument)
  - PG_GETARG_POINT_P (macro for retrieving Point argument)
  - PG_RETURN_PATH_P (macro for returning PATH result)  
  - [point_mul_point](point_mul_point.md) (function for multiplying two points using complex arithmetic)
- Called from:
  - No direct references found in the codebase

## Notes and Other Information
- Uses PG_GETARG_PATH_P_COPY to work on a copy of the input path, ensuring the original is not modified
- Applies the same transformation to all points in the path uniformly
- The transformation can combine scaling and rotation in a single operation
- [Point](../P/Point.md) multiplication follows complex number arithmetic rules
- Part of PostgreSQL's geometric transformation system alongside translation operators
- Preserves path topology while allowing complex shape transformations