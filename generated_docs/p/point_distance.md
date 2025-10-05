# point_distance

## Location
[src/backend/utils/adt/geo_ops.c:1993-2001](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1993-L2001)

## Overview
Calculates the Euclidean distance between two points using the standard distance formula.

## Definition

```c
Datum
point_distance(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL geometric operator that computes the straight-line distance between two points in 2D space. It serves as a SQL-callable wrapper around the internal  function, which implements the standard Euclidean distance formula using the HYPOT function for numerical stability. This function is part of PostgreSQL's geometric arithmetic operators and is used in spatial queries, indexing operations, and distance-based calculations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: Point pointer () - the first point
  - Second argument: Point pointer () - the second point

## Dependencies
- Functions called/Symbols referenced:
  -  - extracts Point arguments from function call
  -  - internal function that calculates Euclidean distance using HYPOT
  -  - returns floating-point result to PostgreSQL
- Called from (representative examples):
  -  (in GiST and SP-GiST indexing procedures)
  -  (in regression tests)

## Notes and Other Information
- Implements the standard Euclidean distance formula: sqrt((x2-x1)² + (y2-y1)²)
- Uses HYPOT function internally for numerical stability and to avoid overflow/underflow
- Part of PostgreSQL's geometric arithmetic operators for distance calculations
- Used extensively in spatial indexing (GiST, SP-GiST) for nearest-neighbor queries
- Returns a float8 (double precision) value representing the distance between the points
- Can be used in SQL queries like  which returns 5.0

## Simplified Source
```c
Datum point_distance(PG_FUNCTION_ARGS) {
    Point *pt1 = PG_GETARG_POINT_P(0);
    Point *pt2 = PG_GETARG_POINT_P(1);

    // Delegate to point_dt for distance calculation
    PG_RETURN_FLOAT8(point_dt(pt1, pt2));
}
```