# box_distance

## Location
[src/backend/utils/adt/geo_ops.c:832-848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L832-L848)

## Overview
Calculates and returns the distance between the center points of two geometric boxes in PostgreSQL.

## Definition


## Detailed Description
The `box_distance` function computes the Euclidean distance between the center points of two BOX geometric data types. It works by first extracting two BOX arguments from the function call, then calculating the center point of each box using the `box_cn` helper function, and finally computing the distance between these two center points using the `point_dt` function.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that contains:
  - BOX pointer (argument 0): The first box geometry
  - BOX pointer (argument 1): The second box geometry

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BOX_P`: PostgreSQL macro to extract BOX pointer from function arguments
  - [box_cn](box_cn.md): Function to calculate the center point of a box
  - [point_dt](../p/point_dt.md): Function to calculate the distance between two points
  - `PG_RETURN_FLOAT8`: PostgreSQL macro to return a float8 value
  - [Point](../P/Point.md): PostgreSQL geometric point data type
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operations located in `src/backend/utils/adt/geo_ops.c:832-848`
- The function creates two temporary Point variables to store the center coordinates of each box
- Returns a float8 (double precision) value representing the distance between box centers
- Part of the geometric operations suite for the BOX data type in PostgreSQL
- The distance calculation is performed using standard Euclidean distance formula via `point_dt`