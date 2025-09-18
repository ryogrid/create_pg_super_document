# lseg_closept_point

## Location
[src/backend/utils/adt/geo_ops.c:2772-2790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2772-L2790)

## Overview
Calculates the closest point on a line segment to a specified point and returns the distance between them.

## Definition


## Detailed Description
This static function finds the closest point on a line segment to a given point by constructing a perpendicular line from the point to the line segment. It uses geometric algorithms to determine the optimal point on the segment that minimizes the distance to the target point. The function can optionally store the closest point coordinates in the result parameter and always returns the calculated distance.

## Parameters / Member Variables
- : Output parameter - pointer to Point structure where the closest point coordinates will be stored (can be NULL if only distance is needed)
- : Input line segment (LSEG structure) on which to find the closest point
- : Input point for which to find the closest point on the line segment

## Dependencies
- Functions called/Symbols referenced:
  - [line_construct](line_construct.md): Constructs a line from two points
  - [point_invsl](../p/point_invsl.md): Calculates inverse slope between two points
  - [lseg_closept_line](lseg_closept_line.md): Finds closest point on line segment to a line
  - [point_dt](../p/point_dt.md): Calculates distance between two points
- Called from (representative examples):
  - [dist_ps](../d/dist_ps.md): Distance from point to line segment
  - [dist_sp](../d/dist_sp.md): Distance from line segment to point
  - [close_ps](../c/close_ps.md): Closest point from point to line segment
  - [lseg_closept_lseg](lseg_closept_lseg.md): Closest point between two line segments
  - [box_closept_point](../b/box_closept_point.md): Closest point on box to a point

## Notes and Other Information
- This is a static function used internally within geo_ops.c for geometric calculations
- The algorithm constructs a perpendicular line from the input point to find the optimal closest point
- Used extensively in distance calculations and closest point operations for various geometric types
- Returns float8 (double precision) distance value