# dist_pathp

## Location
src/backend/utils/adt/geo_ops.c: 2490 - 2501

## Overview
PostgreSQL SQL function that calculates the distance from a PATH to a Point (reverse parameter order of dist_ppath).

## Definition


## Detailed Description
This function serves as the public PostgreSQL SQL interface for computing the shortest distance from a path to a point. It extracts the path and point arguments from the function call framework and delegates the actual distance calculation to . The function is functionally equivalent to  but with reversed argument order, demonstrating that distance between a point and path is commutative.

## Parameters / Member Variables
- : PostgreSQL function call framework arguments containing:
  - Argument 0: PATH pointer () - the path from which to measure distance
  - Argument 1: Point pointer () - the point to which distance is measured

## Dependencies
- Functions called/Symbols referenced:
  -  - PostgreSQL macro to extract PATH argument
  -  - PostgreSQL macro to extract Point argument
  -  - Internal function that performs the actual distance calculation
  -  - PostgreSQL macro to return float8 result
- Called from (representative examples):
  - No direct callers found in codebase (likely called via SQL function dispatch)

## Notes and Other Information
- Located in 
- This is a PostgreSQL SQL callable function for geometric operations
- Acts as a thin wrapper around 
- Functionally identical to  but with reversed parameter order
- Returns the result as a float8 (double precision) value
- Demonstrates the commutative property of point-to-path distance
- Part of PostgreSQL's geometric data type system