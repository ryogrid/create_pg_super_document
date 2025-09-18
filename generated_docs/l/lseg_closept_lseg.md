# lseg_closept_lseg

## Location
src/backend/utils/adt/geo_ops.c: 2810 - 2852

## Overview
Calculates the closest point between two line segments and returns the minimum distance between them.

## Definition


## Detailed Description
This static function determines the closest point between two line segments using a comprehensive algorithm. It first checks if the line segments intersect (in which case the distance is 0). If they don't intersect, it systematically compares distances from all endpoints of one segment to the other segment to find the minimum distance. The algorithm ensures that all possible closest point configurations are considered: endpoints of either segment to the other segment.

## Parameters / Member Variables
- : Output parameter - pointer to Point structure where the closest point coordinates will be stored (can be NULL if only distance is needed)
- : First line segment (the segment on which the closest point will be found)
- : Second line segment (the segment to which we're finding the closest point)

## Dependencies
- Functions called/Symbols referenced:
  - lseg_interpt_lseg: Checks if two line segments intersect and returns intersection point
  - lseg_closept_point: Finds closest point on line segment to a point (called multiple times)
  - float8_lt: Compares two float8 values for less-than relationship
- Called from (representative examples):
  - path_distance: Distance calculation between paths
  - lseg_distance: Distance between two line segments
  - close_lseg: Closest point function for line segments
  - box_closept_lseg: Closest point from box to line segment
  - poly_distance: Distance calculation for polygons

## Notes and Other Information
- This is a static function used internally within geo_ops.c for geometric calculations
- The algorithm handles intersecting segments as a special case (distance = 0)
- Systematically checks all four endpoint-to-segment combinations to ensure optimal result
- Used extensively in distance calculations between various geometric types involving line segments
- Returns float8 (double precision) distance value
- Critical for PostgreSQL's geometric operations involving line segment proximity