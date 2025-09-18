# path_inter

## Location
[src/backend/utils/adt/geo_ops.c:1653-1729](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1653-L1729)

## Overview
The `path_inter` function determines whether two paths intersect at any point using bounding box optimization followed by pairwise segment intersection checks.

## Definition
```c
Datum path_inter(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_inter` function implements a two-phase algorithm to detect intersections between two PATH geometric objects. First, it performs a quick O(n) bounding box check by calculating the minimum bounding rectangles for both paths and testing if they overlap using `box_ov`. If the bounding boxes don't overlap, the paths cannot intersect and the function returns false immediately. If the bounding boxes do overlap, it proceeds to a more expensive O(n²) pairwise check of all line segments from both paths to determine if any segments actually intersect using `lseg_interpt_lseg`.

The function handles both open and closed paths correctly - for closed paths, it includes the implicit closing segment that connects the last point back to the first point in the intersection analysis.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing two path parameters to test for intersection

## Dependencies
- Functions called/Symbols referenced:
  - [PATH](../P/PATH.md): Geometric path data type structure
  - `PG_GETARG_PATH_P`: Macro to get path arguments from function parameters
  - [BOX](../B/BOX.md): Bounding box data type for optimization
  - [LSEG](../L/LSEG.md): Line segment data type for intersection testing
  - [float8_max](../f/float8_max.md)/`float8_min`: Float comparison utilities for bounding box calculation
  - [box_ov](../b/box_ov.md): Function to test bounding box overlap
  - [statlseg_construct](../s/statlseg_construct.md): Function to construct line segments from points
  - [lseg_interpt_lseg](../l/lseg_interpt_lseg.md): Function to test line segment intersection
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Uses a two-phase optimization strategy: fast bounding box check followed by detailed segment intersection
- Correctly handles both open paths (polylines) and closed paths (polygons)
- For closed paths, includes the implicit closing segment in intersection calculations
- Time complexity is O(n) for bounding box check, O(n²) for full intersection test
- Returns boolean result indicating whether any intersection exists
- Part of PostgreSQL's geometric intersection operations suite