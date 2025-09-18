# poly_same

## Location
src/backend/utils/adt/geo_ops.c: 3720 - 3743

## Overview
Tests whether two polygons are identical by comparing all their points in both forward and reverse directions.

## Definition


## Detailed Description
The `poly_same` function determines if two polygons are geometrically identical by comparing all their vertices. The function first performs a quick check to ensure both polygons have the same number of points, then uses the `plist_same` helper function to perform a comprehensive point-by-point comparison.

The comparison is sophisticated because polygons are non-directional closed shapes, meaning they can be considered the same even if their vertices are ordered in different directions (clockwise vs counterclockwise) or start from different points. The `plist_same` function handles these cases by checking matches in both forward and reverse directions.

This function is part of PostgreSQL's geometric operators and is used to test complete geometric equality between polygon objects.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument: POLYGON pointer (polygon A)
  - Second argument: POLYGON pointer (polygon B)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P: Extracts polygon arguments from function call
  - [plist_same](plist_same.md): Performs detailed point-by-point comparison handling directional variations
  - PG_FREE_IF_COPY: Frees memory for toasted inputs
  - PG_RETURN_BOOL: Returns boolean result
- Called from (representative examples):
  - No direct references found in current codebase

## Notes and Other Information
- The function performs an initial optimization by checking if the number of points differs
- Uses `plist_same` for sophisticated polygon comparison that accounts for non-directional nature
- Handles memory management by freeing toasted inputs to prevent memory leaks
- Part of PostgreSQL's geometric data type operator family for equality testing
- Essential for polygon equality operations in spatial queries and indexing