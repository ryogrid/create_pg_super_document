# lseg_closept_line

## Location
src/backend/utils/adt/geo_ops.c: 2960 - 2987

## Overview
The `lseg_closept_line` function finds the closest point on a line segment to a line and returns the distance between them.

## Definition
```c
static float8 lseg_closept_line(Point *result, LSEG *lseg, LINE *line)
```

## Detailed Description
This static function computes the closest point on a line segment to an infinite line. It first checks if the line segment intersects the line - if so, the distance is 0 and the intersection point is returned. If there's no intersection, it calculates the distances from both endpoints of the line segment to the line and returns the endpoint with the smaller distance. The function handles edge cases where lines are parallel, coordinates contain NaN or infinite values, or rounding errors occur, noting that there may not be a single closest point in such cases.

## Parameters / Member Variables
- `result`: Output parameter - pointer to Point where the closest point will be stored (can be NULL if only distance is needed)
- `lseg`: Input line segment to find the closest point on
- `line`: Input infinite line to measure distance to
- Returns: `float8` - the distance between the line and the closest point on the line segment

## Dependencies
- Functions called/Symbols referenced:
  - `Point` - Point data type definition
  - `LSEG` - Line segment data type definition  
  - `LINE` - Line data type definition
  - `lseg_interpt_line` - Checks for intersection between line segment and line
  - `line_closept_point` - Calculates distance from a point to a line
- Called from (representative examples):
  - `dist_sl` - Distance between line segment and line
  - `dist_ls` - Distance between line and line segment
  - `lseg_closept_point` - Line segment closest point to point calculation
  - `close_ls` - Closest point on line segment to line

## Notes and Other Information
- This is a static (internal) function within the geometric operations module
- The function includes detailed comments about handling parallel lines and edge cases with NaN/infinite coordinates
- When lines are parallel or in degenerate cases, the function may default to returning the second endpoint of the line segment
- Part of PostgreSQL's comprehensive geometric data type support system
- Located in `geo_ops.c` at lines 2960-2987