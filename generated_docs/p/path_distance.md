# path_distance

## Location
[src/backend/utils/adt/geo_ops.c:1730-1791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1730-L1791)

## Overview
The `path_distance` function calculates the minimum distance between any two line segments across two different paths by performing a cartesian product comparison.

## Definition
```c
Datum path_distance(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_distance` function computes the shortest distance between two PATH geometric objects by systematically comparing all possible pairs of line segments from each path. It performs a cartesian product operation across all segments in both paths, using `lseg_closept_lseg` to calculate the distance between each pair of segments, and tracking the minimum distance found.

The function handles both open and closed paths correctly - for closed paths, it includes the implicit closing segment that connects the last point back to the first point in the distance calculations. If no valid segments are found for comparison (which can happen with certain open path configurations), the function returns NULL.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing two path parameters for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  - [PATH](../P/PATH.md): Geometric path data type structure
  - `PG_GETARG_PATH_P`: Macro to get path arguments from function parameters
  - [LSEG](../L/LSEG.md): Line segment data type for distance calculations
  - [statlseg_construct](../s/statlseg_construct.md): Function to construct line segments from points
  - [lseg_closept_lseg](../l/lseg_closept_lseg.md): Function to calculate distance between two line segments
  - [float8_lt](../f/float8_lt.md): Float comparison utility for minimum tracking
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Uses a brute force O(n×m) algorithm where n and m are the number of points in each path
- Correctly handles both open paths (polylines) and closed paths (polygons)
- For closed paths, includes the implicit closing segment in distance calculations
- Returns NULL if no valid segments exist for comparison
- Returns the minimum distance as a float8 value
- Part of PostgreSQL's geometric distance calculation operations suite
- The cartesian product approach ensures the globally minimum distance is found

## Simplified Source

```c
Datum path_distance(PG_FUNCTION_ARGS) {
    // Extract two PATH objects from function arguments
    PATH *p1 = PG_GETARG_PATH_P(0);
    PATH *p2 = PG_GETARG_PATH_P(1);
    float8 min_distance = 0.0;
    bool have_min = false;

    // Compare all segment pairs between the two paths
    for (int i = 0; i < p1->npts; i++) {
        // Determine previous point index for path1 segment
        int iprev = (i > 0) ? i - 1 : (p1->closed ? p1->npts - 1 : -1);
        if (iprev == -1) continue; // Skip if open path and at first point

        for (int j = 0; j < p2->npts; j++) {
            // Determine previous point index for path2 segment
            int jprev = (j > 0) ? j - 1 : (p2->closed ? p2->npts - 1 : -1);
            if (jprev == -1) continue; // Skip if open path and at first point

            // Create line segments from consecutive points
            LSEG seg1, seg2;
            statlseg_construct(&seg1, &p1->p[iprev], &p1->p[i]);
            statlseg_construct(&seg2, &p2->p[jprev], &p2->p[j]);

            // Calculate distance between the two segments
            float8 distance = lseg_closept_lseg(NULL, &seg1, &seg2);

            // Track minimum distance found
            if (!have_min || float8_lt(distance, min_distance)) {
                min_distance = distance;
                have_min = true;
            }
        }
    }

    // Return NULL if no segments were compared
    if (!have_min)
        PG_RETURN_NULL();

    PG_RETURN_FLOAT8(min_distance);
}
```