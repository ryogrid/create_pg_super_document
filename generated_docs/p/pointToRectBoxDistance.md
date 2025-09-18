# pointToRectBoxDistance

## Location
src/backend/utils/adt/geo_spgist.c: 374 - 400

## Overview
A static function that calculates the lower bound distance between a point and a rectangular bounding box in 2D space.

## Definition
```c
static double pointToRectBoxDistance(Point *point, RectBox *rect_box)
```

## Detailed Description
The `pointToRectBoxDistance` function computes the minimum Euclidean distance between a given point and a rectangular bounding box. This function is essential for spatial indexing operations in PostgreSQL's SP-GiST implementation, particularly for distance-based queries and nearest neighbor searches.

The algorithm works by:
1. Calculating the horizontal distance (dx) - if the point's x-coordinate is outside the rectangle's x-range, it computes the distance to the nearest x-boundary; otherwise dx is 0
2. Calculating the vertical distance (dy) - if the point's y-coordinate is outside the rectangle's y-range, it computes the distance to the nearest y-boundary; otherwise dy is 0  
3. Returning the Euclidean distance using the HYPOT function to compute sqrt(dx² + dy²)

If the point is inside the rectangle, both dx and dy will be 0, resulting in a distance of 0. If the point is outside, the function returns the shortest distance to any point on the rectangle's boundary.

## Parameters / Member Variables
- `point`: A pointer to a Point structure representing the query point with x and y coordinates
- `rect_box`: A pointer to a RectBox structure containing the rectangular bounding box with x and y range boundaries

## Dependencies
- Functions called/Symbols referenced:
  - HYPOT (macro for computing hypotenuse)
  - [Point](../P/Point.md) (type)
  - RectBox (type)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md) (multiple locations)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the geo_spgist.c file
- The function returns a double-precision floating point value representing the distance
- Used extensively in SP-GiST inner node consistency checking for distance-based spatial queries
- The distance calculation is optimized to avoid unnecessary square root operations when possible
- Essential for implementing efficient nearest neighbor searches in geometric indexes
- The function handles edge cases where the point lies within the rectangle bounds (returns 0 distance)