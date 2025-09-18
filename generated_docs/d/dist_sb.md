# dist_sb

## Location
src/backend/utils/adt/geo_ops.c: 2550 - 2561

## Overview
Calculates the shortest distance from a line segment (lseg) to a box (rectangle) in 2D coordinate space.

## Definition
```c
Datum dist_sb(PG_FUNCTION_ARGS)
```

## Detailed Description
The `dist_sb` function is a PostgreSQL built-in function that computes the minimum distance between a line segment and a box. It utilizes the `box_closept_lseg` function to find the closest point between these geometric objects and returns the distance as a float8 value. This function is crucial for spatial analysis involving linear and rectangular geometric relationships, commonly used in geographic information systems and spatial indexing operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function call context containing:
  - Argument 0: `LSEG *lseg` - The source line segment for distance calculation
  - Argument 1: `BOX *box` - The target box (rectangle) for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_LSEG_P` - Extracts LSEG argument from function call
  - `PG_GETARG_BOX_P` - Extracts BOX argument from function call
  - `[box_closept_lseg](../b/box_closept_lseg.md)` - Computes closest point between box and line segment
  - `PG_RETURN_FLOAT8` - Returns float8 result
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:2550-2561
- This function follows PostgreSQL's V1 calling convention
- Handles complex geometric relationships including intersection, containment, and separation cases
- Returns 0.0 when the line segment intersects or is contained within the box
- Part of PostgreSQL's geometric operator family enabling sophisticated spatial queries
- Useful for proximity analysis between linear features and rectangular regions in spatial databases