# box_closept_point

## Location
[src/backend/utils/adt/geo_ops.c:2878-2932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2878-L2932)

## Overview
Calculates the closest point on or within a box to a specified point and returns the distance between them.

## Definition

```c
struct(&lseg, &box->low, &point);
```
## Detailed Description
This static function finds the closest point on a box (rectangle) to a given point using a comprehensive algorithm. It first checks if the point is inside the box (in which case the distance is 0 and the closest point is the point itself). If the point is outside the box, it systematically checks all four edges of the box by constructing line segments for each edge and finding the closest point on each edge to the input point. The function returns the minimum distance found among all four edges.

## Parameters / Member Variables
- : Output parameter - pointer to Point structure where the closest point coordinates will be stored (can be NULL if only distance is needed)
- : Input BOX structure representing the rectangle on which to find the closest point
- : Input point for which to find the closest point on the box

## Dependencies
- Functions called/Symbols referenced:
  - [box_contain_point](box_contain_point.md): Checks if a point is contained within the box
  - [statlseg_construct](../s/statlseg_construct.md): Constructs a line segment between two points (called for each box edge)
  - [lseg_closept_point](../l/lseg_closept_point.md): Finds closest point on line segment to a point (called for each edge)
  - [float8_lt](../f/float8_lt.md): Compares two float8 values for less-than relationship
- Called from (representative examples):
  - [dist_pb](../d/dist_pb.md): Distance from point to box
  - [dist_bp](../d/dist_bp.md): Distance from box to point
  - [close_pb](../c/close_pb.md): Closest point from point to box

## Notes and Other Information
- This is a static function used internally within geo_ops.c for geometric calculations
- The algorithm handles the special case when the point is inside the box (distance = 0)
- Systematically checks all four edges of the box to ensure optimal result
- Each edge is constructed as a line segment and processed using existing line segment closest point logic
- The four edges checked are: bottom, top, left, and right edges of the box
- Used extensively in distance calculations between points and rectangular regions
- Returns float8 (double precision) distance value
- Critical for PostgreSQL's geometric operations involving box proximity calculations