# plist_same

## Location
[src/backend/utils/adt/geo_ops.c:5457-5518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5457-L5518)

## Overview
A static utility function that determines if two point lists represent the same geometric shape, accounting for different starting points and orientations (clockwise vs counterclockwise).

## Definition

```c
static bool
plist_same(int npts, Point *p1, Point *p2)
```
## Detailed Description
This function compares two point lists to determine if they represent the same polygon, even if the points are ordered differently. It handles two common variations:
1. **Different starting points**: The same polygon can be represented starting from any vertex
2. **Different orientations**: The same polygon can be traversed clockwise or counterclockwise

The algorithm works by:
1. Finding a matching point in p2 for the first point of p1
2. Once a match is found, checking if all remaining points match in forward direction (same orientation)
3. If forward matching fails, checking if all remaining points match in backward direction (opposite orientation)
4. Returning true if either direction produces a complete match

## Parameters / Member Variables
- `npts`: Number of points in both point lists
- `*p1`: First point list to compare
- `*p2`: Second point list to compare
## Dependencies
- Functions called/Symbols referenced:
  - [Point](../P/Point.md) (geometric point type)
  - [point_eq_point](point_eq_point.md) (point equality comparison function)
- Called from (representative examples):
  - [poly_same](poly_same.md) (polygon equality comparison)
  - [PATH_CLOSED](../P/PATH_CLOSED.md) (path operations)

## Notes and Other Information
- This is a static function, only accessible within geo_ops.c
- Assumes both point lists have the same number of points (npts)
- Uses circular indexing to handle wraparound when checking forward/backward sequences
- Critical for geometric operations that need to identify equivalent polygons regardless of representation
- Part of PostgreSQL's geometric data type support system