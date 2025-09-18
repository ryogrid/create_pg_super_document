# path_div_pt

## Location
src/backend/utils/adt/geo_ops.c: 4438 - 4451

## Overview
Applies inverse rotation and scaling transformation to a path by dividing each point by a transformation point, providing the inverse operation to path multiplication.

## Definition
```c
Datum path_div_pt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_div_pt` function performs inverse rotation and scaling operations on a PATH by dividing each point in the path by a transformation Point. This operation is the inverse of `path_mul_pt`, treating the transformation point as a complex number and applying complex division to achieve inverse scaling and rotation transformations. The function operates in-place on a copy of the input path, applying the same inverse transformation to all points uniformly.

This geometric transformation operator enables reversing previous scaling and rotation operations, or applying inverse transformations directly. The division operation follows complex number arithmetic where points are treated as complex numbers (x + yi), providing precise inverse transformations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `path`: The PATH to be transformed (copied for modification)  
  - `point`: The Point representing the transformation parameters for inverse scaling/rotation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P_COPY (macro for retrieving and copying PATH argument)
  - PG_GETARG_POINT_P (macro for retrieving Point argument)
  - PG_RETURN_PATH_P (macro for returning PATH result)
  - [point_div_point](point_div_point.md) (function for dividing two points using complex arithmetic)
- Called from:
  - No direct references found in the codebase

## Notes and Other Information
- Uses PG_GETARG_PATH_P_COPY to work on a copy of the input path, ensuring the original is not modified
- Applies the same inverse transformation to all points in the path uniformly
- The transformation provides inverse scaling and rotation operations
- [Point](../P/Point.md) division follows complex number arithmetic rules
- Inverse operation to path_mul_pt, allowing bidirectional scaling/rotation transformations
- Part of PostgreSQL's comprehensive geometric transformation system
- Preserves path topology while enabling complex inverse shape transformations