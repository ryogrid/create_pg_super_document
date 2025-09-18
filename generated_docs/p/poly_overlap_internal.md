# poly_overlap_internal

## Location
[src/backend/utils/adt/geo_ops.c:3744-3800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3744-L3800)

## Overview
Internal function that determines if two polygons overlap using bounding box checks and edge intersection algorithms.

## Definition


## Detailed Description
The `poly_overlap_internal` function is a comprehensive algorithm for determining polygon overlap using a multi-stage approach. It first performs a quick bounding box overlap check using `box_ov`, and if the bounding boxes overlap, it proceeds with a more detailed geometric analysis.

The detailed analysis uses a brute-force approach that checks for edge intersections between the two polygons. It iterates through all edges of both polygons, treating each edge as a line segment (LSEG), and tests for intersections using `lseg_interpt_lseg`. If no edge intersections are found, it performs a final check to determine if one polygon is completely contained within the other using the `point_inside` function.

This function serves as the core implementation for polygon overlap detection and is used by higher-level functions like `poly_overlap` and `poly_distance`.

## Parameters / Member Variables
- `polya`: Pointer to the first POLYGON structure to test
- `polyb`: Pointer to the second POLYGON structure to test

## Dependencies
- Functions called/Symbols referenced:
  - [box_ov](../b/box_ov.md): Performs bounding box overlap check for quick elimination
  - [lseg_interpt_lseg](../l/lseg_interpt_lseg.md): Tests if two line segments intersect
  - [point_inside](point_inside.md): Determines if a point is inside a polygon
  - Assert: Validates that both polygons have points
- Called from (representative examples):
  - [poly_overlap](poly_overlap.md): Public interface function for overlap testing
  - [poly_distance](poly_distance.md): Distance calculation function that needs overlap information

## Notes and Other Information
- Static function - not exposed in the public API, used internally within geo_ops.c
- Uses a two-phase algorithm: quick bounding box check followed by detailed geometric analysis
- Brute-force edge intersection testing has O(n*m) complexity where n and m are the number of edges
- Handles containment cases where one polygon is completely inside another
- Essential for accurate polygon overlap detection beyond simple bounding box comparisons
- Assumes input polygons have at least one point (validated by Assert)