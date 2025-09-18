# getQuadrant

## Location
src/backend/utils/adt/rangetypes_spgist.c: 95 - 130

## Overview
Determines which quadrant a point falls into relative to a centroid point, used in SP-GiST quadtree indexing operations.

## Definition
static int16 getQuadrant(Point *centroid, Point *tst)

## Detailed Description
This function implements the core quadrant determination logic for SP-GiST quadtree operations. It takes a centroid point and a test point, then determines which of the four quadrants the test point falls into relative to the centroid. The quadrants are numbered 1-4 in a specific pattern: quadrant 1 (upper right), quadrant 2 (lower right), quadrant 3 (lower left), and quadrant 4 (upper left). Points lying exactly on the axes are assigned to the lowest-numbered adjacent quadrant to ensure consistent placement.

## Parameters / Member Variables
- centroid: Pointer to the central reference point that defines the quadrant boundaries
- tst: Pointer to the test point whose quadrant position needs to be determined

## Dependencies
- Functions called/Symbols referenced:
  - Point (structure type)
  - SPTEST (macro for spatial tests)
  - point_above, point_below, point_left, point_right (spatial comparison functions)
  - point_horiz, point_vert (axis alignment test functions)
  - elog (error logging function)
- Called from (representative examples):
  - spg_quad_choose
  - spg_quad_picksplit
  - spg_range_quad_choose
  - spg_range_quad_picksplit

## Notes and Other Information
- Quadrant numbering follows a specific pattern: 4|1 over 3|2 (counter-clockwise from upper right)
- Points on axes are placed in the lowest-numbered adjacent quadrant for consistency
- Uses SPTEST macro extensively for robust spatial comparisons
- Returns an error if no quadrant can be determined (should be impossible case)
- Essential component of SP-GiST quadtree splitting and navigation algorithms
- Located in src/backend/access/spgist/spgquadtreeproc.c:55-82