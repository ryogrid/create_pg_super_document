# make_bound_box

## Location
[src/backend/utils/adt/geo_ops.c:3376-3414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3376-L3414)

## Overview
The `make_bound_box` function creates the smallest bounding box for a given polygon by calculating the minimum and maximum x and y coordinates from all vertices.

## Definition
```c
static void make_bound_box(POLYGON *poly)
```

## Detailed Description
This is a static utility function that computes the axis-aligned bounding box for a polygon data structure. It iterates through all points in the polygon to find the minimum and maximum x and y coordinates, then stores these values in the polygons `boundbox` field. The bounding box is essential for spatial operations and optimizations, allowing quick spatial queries and comparisons without examining all polygon vertices.

The function uses PostgreSQLs float8 comparison functions to handle floating-point comparisons safely, ensuring consistent behavior across different platforms and avoiding potential floating-point precision issues.

## Parameters / Member Variables
- `poly`: Pointer to a POLYGON structure containing the polygon data whose bounding box needs to be calculated. The polygon must have at least one point (npts > 0).

## Dependencies
- Functions called/Symbols referenced:
  - [float8_lt](../f/float8_lt.md): Less-than comparison for float8 values
  - [float8_gt](../f/float8_gt.md): Greater-than comparison for float8 values
  - [POLYGON](../P/POLYGON.md): Polygon data structure type
- Called from (representative examples):
  - [poly_in](../p/poly_in.md): Called during polygon input parsing
  - [poly_recv](../p/poly_recv.md): Called during polygon binary deserialization
  - [path_poly](../p/path_poly.md): Called when converting path to polygon
  - [circle_poly](../c/circle_poly.md): Called when converting circle to polygon

## Notes and Other Information
- The function is static, indicating its only used within the geo_ops.c file
- Requires the polygon to have at least one point (enforced by Assert)
- The bounding box is stored directly in the polygon structures boundbox field
- Uses safe float8 comparison functions rather than direct < and > operators
- Part of PostgreSQLs geometric data type implementation

## Simplified Source

```c
static void make_bound_box(POLYGON *poly) {
    // Initialize bounds with first point coordinates
    float8 x1 = poly->p[0].x;
    float8 y1 = poly->p[0].y;
    float8 x2 = poly->p[0].x;
    float8 y2 = poly->p[0].y;

    // Find minimum and maximum x,y coordinates across all points
    for (int i = 1; i < poly->npts; i++) {
        if (poly->p[i].x < x1) x1 = poly->p[i].x;  // Update min x
        if (poly->p[i].x > x2) x2 = poly->p[i].x;  // Update max x
        if (poly->p[i].y < y1) y1 = poly->p[i].y;  // Update min y
        if (poly->p[i].y > y2) y2 = poly->p[i].y;  // Update max y
    }

    // Store the calculated bounding box in the polygon structure
    poly->boundbox.low.x = x1;
    poly->boundbox.high.x = x2;
    poly->boundbox.low.y = y1;
    poly->boundbox.high.y = y2;
}
```