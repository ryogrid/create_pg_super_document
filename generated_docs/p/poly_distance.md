# poly_distance

## Location
[src/backend/utils/adt/geo_ops.c:4027-4095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4027-L4095)

## Overview
PostgreSQL function that calculates the minimum distance between two polygons, returning zero if they overlap or the minimum edge-to-edge distance otherwise.

## Definition

```c
struct(&seg1, &polya->p[iprev], &polya->p[i]);
```
## Detailed Description
This function computes the shortest distance between two polygons using a comprehensive algorithm. First, it checks if the polygons overlap using poly_overlap_internal - if they do, the distance is zero since overlapping polygons have no separation. For non-overlapping polygons, it calculates the minimum distance by examining all possible pairs of edges between the two polygons. The algorithm constructs line segments (LSEG) for each edge of both polygons and uses lseg_closept_lseg to find the closest point distance between each pair of segments. The computation is similar to path distance calculation but treats polygons as closed paths. The function maintains the minimum distance found across all edge pair comparisons.

## Parameters / Member Variables
- : PostgreSQL function call context containing two arguments
  - Argument 0: First polygon (POLYGON) for distance calculation
  - Argument 1: Second polygon (POLYGON) for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P (extract polygon arguments)
  - [poly_overlap_internal](poly_overlap_internal.md) (check if polygons overlap)
  - [statlseg_construct](../s/statlseg_construct.md) (construct line segments from polygon edges)
  - [lseg_closept_lseg](../l/lseg_closept_lseg.md) (calculate closest point distance between line segments)
  - [float8_lt](../f/float8_lt.md) (floating-point comparison)
  - PG_RETURN_FLOAT8 (return distance result)
  - PG_RETURN_NULL (return null if no valid distance)
- Called from (representative examples):
  - No direct references found in the codebase (likely used via SQL distance operator <->)

## Notes and Other Information
- The function is part of PostgreSQL's geometric data type operations in src/backend/utils/adt/geo_ops.c
- Returns 0.0 immediately for overlapping polygons to avoid incorrect path distance calculations
- Uses the same distance algorithm as closed paths since polygon containment areas don't affect edge distances
- Performs O(n*m) comparisons where n and m are the vertex counts of the two polygons
- Located at src/backend/utils/adt/geo_ops.c:4027-4095
- Returns NULL if no valid minimum distance can be determined (though this case should be rare)
- The algorithm handles polygon closure by connecting the last vertex back to the first vertex of each polygon

## Simplified Source

```c
Datum poly_distance(PG_FUNCTION_ARGS) {
    POLYGON *polya = PG_GETARG_POLYGON_P(0);
    POLYGON *polyb = PG_GETARG_POLYGON_P(1);
    float8 min = 0.0;
    bool have_min = false;
    float8 tmp;
    int i, j;
    LSEG seg1, seg2;

    // If polygons overlap, distance is zero
    if (poly_overlap_internal(polya, polyb))
        PG_RETURN_FLOAT8(0.0);

    // Compare all edge pairs between the two polygons
    for (i = 0; i < polya->npts; i++) {
        int iprev = (i > 0) ? i - 1 : polya->npts - 1;  // Previous vertex

        for (j = 0; j < polyb->npts; j++) {
            int jprev = (j > 0) ? j - 1 : polyb->npts - 1;  // Previous vertex

            // Construct edge segments for both polygons
            statlseg_construct(&seg1, &polya->p[iprev], &polya->p[i]);
            statlseg_construct(&seg2, &polyb->p[jprev], &polyb->p[j]);

            // Calculate closest distance between these edges
            tmp = lseg_closept_lseg(NULL, &seg1, &seg2);
            if (!have_min || float8_lt(tmp, min)) {
                min = tmp;
                have_min = true;
            }
        }
    }

    if (!have_min)
        PG_RETURN_NULL();

    PG_RETURN_FLOAT8(min);
}
```