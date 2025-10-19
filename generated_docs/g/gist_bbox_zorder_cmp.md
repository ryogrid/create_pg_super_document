# gist_bbox_zorder_cmp

## Location
[src/backend/access/gist/gistproc.c:1681-1713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L1681-L1713)

## Overview
A static comparison function that compares two bounding boxes using their Z-order (Morton code) values for efficient spatial sorting during GiST index construction.

## Definition
```c
static int gist_bbox_zorder_cmp(Datum a, Datum b, SortSupport ssup)
```

## Detailed Description
This function implements a comparison operation based on Z-order (Morton code) values for spatial sorting of bounding boxes during GiST index construction. It extracts the lower-left corner points from two bounding boxes and computes their respective Z-order values using the Morton code algorithm. The function performs an optimization by first checking for exact coordinate equality before computing the more expensive Z-order values. This comparison function is essential for the fast index build process, as it allows spatial data to be sorted in a way that preserves locality, leading to better index structure and query performance.

## Parameters / Member Variables
- `a`: Datum containing the first bounding box to compare
- `b`: Datum containing the second bounding box to compare  
- `ssup`: SortSupport structure (unused in this function but required by the interface)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetBoxP](../D/DatumGetBoxP.md) (extracts Box pointer from Datum)
  - [point_zorder_internal](../p/point_zorder_internal.md) (computes Z-order value for a point)
- Called from:
  - [gist_point_sortsupport](gist_point_sortsupport.md) (registered as comparison function)

## Notes and Other Information
- Returns -1, 0, or 1 following standard comparison function conventions
- Uses only the lower-left corner (low point) of each bounding box for comparison
- Includes an optimization for exact coordinate equality that avoids Z-order computation
- The equality check is particularly beneficial when used as a tie-breaker with abbreviated keys
- Part of PostgreSQL's GiST fast index build infrastructure that leverages spatial locality
- Z-order comparison enables efficient spatial sorting that maintains locality properties
- The SortSupport parameter follows the PostgreSQL sorting interface but is not used in this implementation

## Simplified Source

```c
static int
gist_bbox_zorder_cmp(Datum a, Datum b, SortSupport ssup)
{
    // Extract lower-left corner points from bounding boxes
    Point *p1 = &(DatumGetBoxP(a)->low);
    Point *p2 = &(DatumGetBoxP(b)->low);

    // Quick equality check to avoid expensive Z-order computation
    if (p1->x == p2->x && p1->y == p2->y)
        return 0;

    // Compute Z-order values for comparison
    uint64 z1 = point_zorder_internal(p1->x, p1->y);
    uint64 z2 = point_zorder_internal(p2->x, p2->y);

    // Return comparison result (-1, 0, or 1)
    if (z1 > z2)
        return 1;
    else if (z1 < z2)
        return -1;
    else
        return 0;
}
```