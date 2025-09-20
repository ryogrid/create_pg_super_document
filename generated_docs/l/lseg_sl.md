# lseg_sl

## Location
[src/backend/utils/adt/geo_ops.c:2155-2164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2155-L2164)

## Overview
Calculates the slope of a line segment by computing the slope between its two endpoints.

## Definition

```c
static inline float8
lseg_sl(LSEG *lseg)
```
## Detailed Description
The  function is a utility function that calculates the slope of a line segment. It acts as a wrapper around the  function, passing the two endpoints of the line segment (lseg->p[0] and lseg->p[1]) to compute the slope. The slope is calculated as the ratio of the vertical distance to the horizontal distance between the two points. Special cases include returning infinity for vertical lines and 0.0 for horizontal lines.

## Parameters / Member Variables
- : Pointer to a line segment (LSEG) structure containing two points p[0] and p[1]

## Dependencies
- Functions called/Symbols referenced:
  - [point_sl](../p/point_sl.md): Calculates slope between two points
  - [LSEG](../L/LSEG.md): Line segment data structure type

- Called from (representative examples):
  - [lseg_parallel](lseg_parallel.md): Checks if two line segments are parallel
  - [lseg_perp](lseg_perp.md): Checks if two line segments are perpendicular
  - [lseg_interpt_lseg](lseg_interpt_lseg.md): Finds intersection point of two line segments
  - [lseg_interpt_line](lseg_interpt_line.md): Finds intersection point of line segment and line
  - [close_lseg](../c/close_lseg.md): Finds closest point on line segment
  - [close_ls](../c/close_ls.md): Finds closest point between line and segment

## Notes and Other Information
- This is a static inline function, meaning it's only accessible within the same source file and is optimized for performance
- Returns infinity for vertical line segments (when x-coordinates are equal)
- Returns 0.0 for horizontal line segments (when y-coordinates are equal)
- Used extensively in geometric calculations involving line segment relationships such as parallelism, perpendicularity, and intersections