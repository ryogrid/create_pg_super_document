# path_inter

## Location
[src/backend/utils/adt/geo_ops.c:1653-1729](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1653-L1729)

## Overview
The `path_inter` function determines whether two paths intersect at any point using bounding box optimization followed by pairwise segment intersection checks.

## Definition
```c
Datum path_inter(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_inter` function implements a two-phase algorithm to detect intersections between two PATH geometric objects. First, it performs a quick O(n) bounding box check by calculating the minimum bounding rectangles for both paths and testing if they overlap using `box_ov`. If the bounding boxes don't overlap, the paths cannot intersect and the function returns false immediately. If the bounding boxes do overlap, it proceeds to a more expensive O(n²) pairwise check of all line segments from both paths to determine if any segments actually intersect using `lseg_interpt_lseg`.

The function handles both open and closed paths correctly - for closed paths, it includes the implicit closing segment that connects the last point back to the first point in the intersection analysis.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing two path parameters to test for intersection

## Dependencies
- Functions called/Symbols referenced:
  - [PATH](../P/PATH.md): Geometric path data type structure
  - `PG_GETARG_PATH_P`: Macro to get path arguments from function parameters
  - [BOX](../B/BOX.md): Bounding box data type for optimization
  - [LSEG](../L/LSEG.md): Line segment data type for intersection testing
  - [float8_max](../f/float8_max.md)/`float8_min`: Float comparison utilities for bounding box calculation
  - [box_ov](../b/box_ov.md): Function to test bounding box overlap
  - [statlseg_construct](../s/statlseg_construct.md): Function to construct line segments from points
  - [lseg_interpt_lseg](../l/lseg_interpt_lseg.md): Function to test line segment intersection
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Uses a two-phase optimization strategy: fast bounding box check followed by detailed segment intersection
- Correctly handles both open paths (polylines) and closed paths (polygons)
- For closed paths, includes the implicit closing segment in intersection calculations
- Time complexity is O(n) for bounding box check, O(n²) for full intersection test
- Returns boolean result indicating whether any intersection exists
- Part of PostgreSQL's geometric intersection operations suite

## Simplified Source

```c
Datum path_inter(PG_FUNCTION_ARGS) {
    // Extract two PATH objects from function arguments
    PATH *p1 = PG_GETARG_PATH_P(0);
    PATH *p2 = PG_GETARG_PATH_P(1);
    BOX b1, b2;

    // Phase 1: Build bounding boxes for quick overlap test
    // Initialize bounding box for path 1
    b1.high.x = b1.low.x = p1->p[0].x;
    b1.high.y = b1.low.y = p1->p[0].y;
    for (int i = 1; i < p1->npts; i++) {
        b1.high.x = float8_max(p1->p[i].x, b1.high.x);
        b1.high.y = float8_max(p1->p[i].y, b1.high.y);
        b1.low.x = float8_min(p1->p[i].x, b1.low.x);
        b1.low.y = float8_min(p1->p[i].y, b1.low.y);
    }

    // Initialize bounding box for path 2
    b2.high.x = b2.low.x = p2->p[0].x;
    b2.high.y = b2.low.y = p2->p[0].y;
    for (int i = 1; i < p2->npts; i++) {
        b2.high.x = float8_max(p2->p[i].x, b2.high.x);
        b2.high.y = float8_max(p2->p[i].y, b2.high.y);
        b2.low.x = float8_min(p2->p[i].x, b2.low.x);
        b2.low.y = float8_min(p2->p[i].y, b2.low.y);
    }

    // Quick rejection: if bounding boxes don't overlap, paths can't intersect
    if (!box_ov(&b1, &b2))
        PG_RETURN_BOOL(false);

    // Phase 2: Check all segment pairs for intersection
    for (int i = 0; i < p1->npts; i++) {
        int iprev = (i > 0) ? i - 1 : (p1->closed ? p1->npts - 1 : -1);
        if (iprev == -1) continue; // Skip if open path and at first point

        for (int j = 0; j < p2->npts; j++) {
            int jprev = (j > 0) ? j - 1 : (p2->closed ? p2->npts - 1 : -1);
            if (jprev == -1) continue; // Skip if open path and at first point

            // Create line segments and test for intersection
            LSEG seg1, seg2;
            statlseg_construct(&seg1, &p1->p[iprev], &p1->p[i]);
            statlseg_construct(&seg2, &p2->p[jprev], &p2->p[j]);

            if (lseg_interpt_lseg(NULL, &seg1, &seg2))
                PG_RETURN_BOOL(true);
        }
    }

    // No segments intersect
    PG_RETURN_BOOL(false);
}
```