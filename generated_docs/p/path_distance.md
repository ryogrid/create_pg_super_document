# path_distance

## Location
src/backend/utils/adt/geo_ops.c: 1730 - 1791

## Overview
The `path_distance` function calculates the minimum distance between any two line segments across two different paths by performing a cartesian product comparison.

## Definition
```c
Datum path_distance(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_distance` function computes the shortest distance between two PATH geometric objects by systematically comparing all possible pairs of line segments from each path. It performs a cartesian product operation across all segments in both paths, using `lseg_closept_lseg` to calculate the distance between each pair of segments, and tracking the minimum distance found.

The function handles both open and closed paths correctly - for closed paths, it includes the implicit closing segment that connects the last point back to the first point in the distance calculations. If no valid segments are found for comparison (which can happen with certain open path configurations), the function returns NULL.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing two path parameters for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  - `PATH`: Geometric path data type structure
  - `PG_GETARG_PATH_P`: Macro to get path arguments from function parameters
  - `LSEG`: Line segment data type for distance calculations
  - `statlseg_construct`: Function to construct line segments from points
  - `lseg_closept_lseg`: Function to calculate distance between two line segments
  - `float8_lt`: Float comparison utility for minimum tracking
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Uses a brute force O(n×m) algorithm where n and m are the number of points in each path
- Correctly handles both open paths (polylines) and closed paths (polygons)
- For closed paths, includes the implicit closing segment in distance calculations
- Returns NULL if no valid segments exist for comparison
- Returns the minimum distance as a float8 value
- Part of PostgreSQL's geometric distance calculation operations suite
- The cartesian product approach ensures the globally minimum distance is found