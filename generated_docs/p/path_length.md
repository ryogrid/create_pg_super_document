# path_length

## Location
src/backend/utils/adt/geo_ops.c: 1792 - 1830

## Overview
The `path_length` function calculates the total length of a path by summing the distances of all line segments that compose the path.

## Definition
```c
Datum path_length(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_length` function computes the total geometric length of a PATH object by iterating through all consecutive point pairs and summing their Euclidean distances using `point_dt`. The function handles both open and closed paths correctly - for open paths, it sums the distances between consecutive points; for closed paths, it additionally includes the distance from the last point back to the first point (the closure segment).

The function uses `float8_pl` for floating-point addition to ensure proper handling of floating-point arithmetic and potential overflow/underflow conditions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing the path parameter for length calculation

## Dependencies
- Functions called/Symbols referenced:
  - `[PATH](../P/PATH.md)`: Geometric path data type structure
  - `PG_GETARG_PATH_P`: Macro to get path argument from function parameters
  - `[point_dt](point_dt.md)`: Function to calculate Euclidean distance between two points
  - `[float8_pl](../f/float8_pl.md)`: Floating-point addition utility for accumulating distances
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Calculates total geometric length by summing segment distances
- Correctly handles both open paths (polylines) and closed paths (polygons)
- For closed paths, includes the implicit closing segment in length calculation
- Uses proper floating-point arithmetic functions for numerical stability
- Returns the total length as a float8 value
- Part of PostgreSQL's geometric "arithmetic" operations suite
- Time complexity is O(n) where n is the number of points in the path