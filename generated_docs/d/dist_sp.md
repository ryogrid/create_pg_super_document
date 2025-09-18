# dist_sp

## Location
[src/backend/utils/adt/geo_ops.c:2426-2434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2426-L2434)

## Overview
Calculates the distance from a line segment (LSEG) to a Point in PostgreSQL's geometric data types.

## Definition


## Detailed Description
This function computes the shortest distance from a given line segment to a point. It serves as a PostgreSQL SQL function that can be called to perform geometric distance calculations. The implementation is functionally equivalent to  but with reversed argument order - it delegates the actual distance computation to the same  function, demonstrating that distance between a point and line segment is commutative.

## Parameters / Member Variables
- : PostgreSQL function call framework arguments containing:
  - Argument 0: Line segment pointer () - the line segment from which to measure distance
  - Argument 1: Point pointer () - the point to which distance is measured

## Dependencies
- Functions called/Symbols referenced:
  -  - PostgreSQL macro to extract LSEG argument
  -  - PostgreSQL macro to extract Point argument
  -  - Core function that calculates closest point distance
  -  - PostgreSQL macro to return float8 result
- Called from (representative examples):
  - No direct callers found in codebase (likely called via SQL function dispatch)

## Notes and Other Information
- Located in 
- This is a PostgreSQL SQL callable function for geometric operations
- Functionally identical to  but with reversed parameter order
- Returns the result as a float8 (double precision) value
- Demonstrates the commutative property of point-to-line-segment distance