# dist_ps

## Location
[src/backend/utils/adt/geo_ops.c:2414-2425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2414-L2425)

## Overview
Calculates the distance from a Point to a line segment (LSEG) in PostgreSQL's geometric data types.

## Definition


## Detailed Description
This function computes the shortest distance from a given point to a line segment. It serves as a PostgreSQL SQL function that can be called to perform geometric distance calculations. The implementation delegates the actual distance computation to the  function, which finds the closest point on the line segment to the given point and returns the distance.

## Parameters / Member Variables
- : PostgreSQL function call framework arguments containing:
  - Argument 0: Point pointer () - the point from which to measure distance
  - Argument 1: Line segment pointer () - the line segment to which distance is measured

## Dependencies
- Functions called/Symbols referenced:
  -  - PostgreSQL macro to extract Point argument
  -  - PostgreSQL macro to extract LSEG argument  
  -  - Core function that calculates closest point distance
  -  - PostgreSQL macro to return float8 result
- Called from (representative examples):
  - No direct callers found in codebase (likely called via SQL function dispatch)

## Notes and Other Information
- Located in 
- This is a PostgreSQL SQL callable function for geometric operations
- Returns the result as a float8 (double precision) value
- The actual distance calculation logic is implemented in 