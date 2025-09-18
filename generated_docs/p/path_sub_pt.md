# path_sub_pt

## Location
src/backend/utils/adt/geo_ops.c: 4409 - 4424

## Overview
Translates a path by subtracting a point offset from all points in the path, effectively moving the entire path by the negative displacement.

## Definition
```c
Datum path_sub_pt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_sub_pt` function performs a translation operation on a PATH by subtracting a Point offset from every point in the path. This is the inverse operation of `path_add_pt`, moving the entire path by the negative of the specified displacement vector. The function operates in-place on a copy of the input path, modifying each point's coordinates by subtracting the corresponding coordinates from the offset point.

This translation operator allows paths to be repositioned in 2D space while maintaining their shape and relative point relationships, providing the subtraction counterpart to the addition translation operation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `path`: The PATH to be translated (copied for modification)
  - `point`: The Point representing the translation offset to subtract

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P_COPY (macro for retrieving and copying PATH argument)
  - PG_GETARG_POINT_P (macro for retrieving Point argument)  
  - PG_RETURN_PATH_P (macro for returning PATH result)
  - point_sub_point (function for subtracting two points)
- Called from:
  - No direct references found in the codebase

## Notes and Other Information
- Uses PG_GETARG_PATH_P_COPY to work on a copy of the input path, ensuring the original is not modified
- Applies the same subtraction offset to all points in the path uniformly
- The translation preserves the path's topology - open/closed status and point relationships remain unchanged
- Inverse operation to path_add_pt, allowing bidirectional translation of paths
- Part of PostgreSQL's comprehensive geometric transformation system for 2D paths