# computeDistance

## Location
[src/backend/access/gist/gistproc.c:1221-1286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L1221-L1286)

## Overview
A static utility function that calculates the minimum distance between a point and a bounding box, with different algorithms for leaf and internal GiST index nodes.

## Definition
```c
static float8 computeDistance(bool isLeaf, BOX *box, Point *point)
```

## Detailed Description
This function implements efficient distance calculation between a point and a bounding box, which is fundamental for distance-based queries in GiST indexes (such as nearest neighbor searches). The function uses different strategies depending on whether it's calculating distance to a leaf node (actual point data) or internal node (bounding box).

For leaf nodes, it performs a simple point-to-point distance calculation. For internal nodes, it uses geometric analysis to determine the minimum distance:
- If the point lies inside the box, the distance is 0
- If the point aligns horizontally with the box (between low.x and high.x), it calculates the vertical distance to the nearest edge
- If the point aligns vertically with the box (between low.y and high.y), it calculates the horizontal distance to the nearest edge  
- If the point is diagonal to the box, it checks all four vertices and returns the minimum distance

This optimization is crucial for efficient index traversal during distance-ordered searches, allowing the query processor to prune branches that cannot contain closer results.

## Parameters / Member Variables
- `isLeaf`: Boolean indicating whether this is a leaf node (true) or internal node (false)
- `box`: Bounding box structure containing low and high coordinate points
- `point`: Query point for which distance is being calculated

## Dependencies
- Functions called/Symbols referenced:
  - `point_point_distance`: Calculates Euclidean distance between two points
  - [float8_mi](../f/float8_mi.md): Floating-point subtraction utility function
  - [BOX](../B/BOX.md): Bounding box structure type
  - [Point](../P/Point.md): Point coordinate structure type
  - `elog`: Error logging for consistency checks
- Called from (representative examples):
  - [gist_point_distance](../g/gist_point_distance.md): Distance calculation for point queries
  - [gist_bbox_distance](../g/gist_bbox_distance.md): Distance calculation for bounding box queries

## Notes and Other Information
- Uses different algorithms optimized for leaf vs. internal node distance calculation
- Implements geometric optimization by checking point-box relationship before expensive vertex distance calculations
- Includes error checking with elog() for inconsistent coordinate values
- Essential for efficient nearest neighbor and distance-ordered query processing in GiST indexes
- Returns 0.0 when point lies inside the bounding box (internal nodes only)
- For diagonal cases, evaluates all four box vertices to find the true minimum distance
- Part of PostgreSQL's geometric distance infrastructure for spatial indexing

## Simplified Source

```c
static float8 computeDistance(bool isLeaf, BOX *box, Point *point) {
    float8 result = 0.0;

    if (isLeaf) {
        // Simple point-to-point distance for leaf nodes
        result = point_point_distance(point, &box->low);
    } else if (point->x <= box->high.x && point->x >= box->low.x &&
               point->y <= box->high.y && point->y >= box->low.y) {
        // Point is inside the box
        result = 0.0;
    } else if (point->x <= box->high.x && point->x >= box->low.x) {
        // Point is above or below the box (aligned horizontally)
        if (point->y > box->high.y)
            result = float8_mi(point->y, box->high.y);
        else if (point->y < box->low.y)
            result = float8_mi(box->low.y, point->y);
    } else if (point->y <= box->high.y && point->y >= box->low.y) {
        // Point is left or right of the box (aligned vertically)
        if (point->x > box->high.x)
            result = float8_mi(point->x, box->high.x);
        else if (point->x < box->low.x)
            result = float8_mi(box->low.x, point->x);
    } else {
        // Point is diagonal - check all four vertices for minimum distance
        Point vertex;
        float8 subresult;

        result = point_point_distance(point, &box->low);

        subresult = point_point_distance(point, &box->high);
        if (result > subresult)
            result = subresult;

        vertex.x = box->low.x;
        vertex.y = box->high.y;
        subresult = point_point_distance(point, &vertex);
        if (result > subresult)
            result = subresult;

        vertex.x = box->high.x;
        vertex.y = box->low.y;
        subresult = point_point_distance(point, &vertex);
        if (result > subresult)
            result = subresult;
    }

    return result;
}
```