# path_add_pt

## Location
src/backend/utils/adt/geo_ops.c: 4396 - 4408

## Overview
Translates a path by adding a point offset to all points in the path, effectively moving the entire path by the specified displacement.

## Definition
```c
Datum path_add_pt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_add_pt` function performs a translation operation on a PATH by adding a Point offset to every point in the path. This is a geometric transformation that moves the entire path by a constant displacement vector. The function operates in-place on a copy of the input path, modifying each point's coordinates by adding the corresponding coordinates from the offset point.

This is one of the translation operators in PostgreSQL's geometric system, allowing paths to be repositioned in 2D space while maintaining their shape and relative point relationships.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `path`: The PATH to be translated (copied for modification)
  - `point`: The Point representing the translation offset

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P_COPY (macro for retrieving and copying PATH argument)
  - PG_GETARG_POINT_P (macro for retrieving Point argument)
  - PG_RETURN_PATH_P (macro for returning PATH result)
  - point_add_point (function for adding two points)
- Called from:
  - No direct references found in the codebase

## Notes and Other Information
- Uses PG_GETARG_PATH_P_COPY to work on a copy of the input path, ensuring the original is not modified
- Applies the same translation offset to all points in the path uniformly
- The translation preserves the path's topology - open/closed status and point relationships remain unchanged
- Part of PostgreSQL's comprehensive geometric transformation system for 2D paths