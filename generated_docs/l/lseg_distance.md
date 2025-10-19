# lseg_distance

## Location
[src/backend/utils/adt/geo_ops.c:2306-2315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2306-L2315)

## Overview
Calculates the minimum distance between two line segments in PostgreSQL's geometric coordinate system.

## Definition

```c
struct(&tmp, &l2->p[0], lseg_sl(l2));
```
## Detailed Description
The  function computes the shortest distance between two line segments (LSEG data type). This function handles the complex geometry of finding the minimum distance between two line segments, which can occur in several scenarios:

1. If the segments intersect, the distance is 0
2. If the segments don't intersect, the closest distance will be from one of the endpoints of one segment to the other segment
3. The distance may be between an endpoint and the interior of the opposite segment, or between two endpoints

The function delegates the actual distance calculation to the  function, which implements the geometric algorithms needed to find the closest point and corresponding distance between two line segments.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: Pointer to first line segment (LSEG)
  - Second argument: Pointer to second line segment (LSEG)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts LSEG argument from function call
  - : Computes the closest point distance between two line segments
  - : Returns double-precision floating-point result
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operators
- Returns the result as a double-precision floating-point number (FLOAT8)
- The underlying algorithm handles all geometric edge cases
- Used for spatial queries and geometric calculations in PostgreSQL databases
- The distance calculation considers the full geometric relationship between the segments, not just endpoint-to-endpoint distances
- Typically used in SQL queries with distance-based predicates for line segment data

## Simplified Source

```c
Datum lseg_distance(PG_FUNCTION_ARGS) {
    // Get two line segments from function arguments
    LSEG *l1 = PG_GETARG_LSEG_P(0);
    LSEG *l2 = PG_GETARG_LSEG_P(1);

    // Calculate minimum distance between the line segments
    // (NULL = don't need closest point, just distance)
    PG_RETURN_FLOAT8(lseg_closept_lseg(NULL, l1, l2));
}
```