# point_horiz

## Location
[src/backend/utils/adt/geo_ops.c:1946-1954](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1946-L1954)

## Overview
Tests whether two points have the same y-coordinate, determining if they lie on a horizontal line.

## Definition


## Detailed Description
The  function is a PostgreSQL geometric operator that checks if two points are horizontally aligned by comparing their y-coordinates. It uses floating-point equality comparison with appropriate tolerance handling through the  function to determine if the points share the same horizontal position.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: Point pointer () - the first point to compare
  - Second argument: Point pointer () - the second point to compare

## Dependencies
- Functions called/Symbols referenced:
  -  - extracts Point arguments from function call
  -  - floating-point equality comparison with tolerance
  -  - returns boolean result to PostgreSQL
- Called from (representative examples):
  -  (in SP-GiST quadtree processing)

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operations
- Uses floating-point comparison with tolerance rather than exact equality
- Primarily used in spatial indexing algorithms like SP-GiST quadtree operations
- Returns true if points have equal y-coordinates, false otherwise