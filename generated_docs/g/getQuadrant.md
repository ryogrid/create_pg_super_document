# getQuadrant

## Location
[src/backend/utils/adt/rangetypes_spgist.c:95-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_spgist.c#L95-L130)

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
  - [Point](../P/Point.md) (structure type)
  - SPTEST (macro for spatial tests)
  - [point_above](../p/point_above.md), point_below, point_left, point_right (spatial comparison functions)
  - [point_horiz](../p/point_horiz.md), point_vert (axis alignment test functions)
  - elog (error logging function)
- Called from (representative examples):
  - [spg_quad_choose](../s/spg_quad_choose.md)
  - [spg_quad_picksplit](../s/spg_quad_picksplit.md)
  - [spg_range_quad_choose](../s/spg_range_quad_choose.md)
  - [spg_range_quad_picksplit](../s/spg_range_quad_picksplit.md)

## Notes and Other Information
- Quadrant numbering follows a specific pattern: 4|1 over 3|2 (counter-clockwise from upper right)
- Points on axes are placed in the lowest-numbered adjacent quadrant for consistency
- Uses SPTEST macro extensively for robust spatial comparisons
- Returns an error if no quadrant can be determined (should be impossible case)
- Essential component of SP-GiST quadtree splitting and navigation algorithms
- Located in src/backend/access/spgist/spgquadtreeproc.c:55-82

## Simplified Source

```c
static int16 getQuadrant(Point *centroid, Point *test_point) {
    // Quadrant 1 (upper right): above/on horizontal AND right/on vertical
    if ((point_above_or_horizontal(test_point, centroid)) &&
        (point_right_or_vertical(test_point, centroid)))
        return 1;

    // Quadrant 2 (lower right): below AND right/on vertical
    if (point_below(test_point, centroid) &&
        (point_right_or_vertical(test_point, centroid)))
        return 2;

    // Quadrant 3 (lower left): below/on horizontal AND left
    if ((point_below_or_horizontal(test_point, centroid)) &&
        point_left(test_point, centroid))
        return 3;

    // Quadrant 4 (upper left): above AND left
    if (point_above(test_point, centroid) &&
        point_left(test_point, centroid))
        return 4;

    // This should never happen
    elog(ERROR, "getQuadrant: impossible case");
    return 0;
}
```