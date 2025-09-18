# poly_below

## Location
[src/backend/utils/adt/geo_ops.c:3625-3647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3625-L3647)

## Overview
Determines if polygon A is strictly below polygon B by comparing their vertical bounding box coordinates.

## Definition
```c
Datum poly_below(PG_FUNCTION_ARGS)
```

## Detailed Description
The `poly_below` function implements a geometric comparison operator that checks whether one polygon is positioned strictly below another polygon. It performs this check by comparing the bounding boxes of the two polygons along the Y-axis - specifically, it returns true if the uppermost point (high.y) of polygon A is less than the lowermost point (low.y) of polygon B. This provides efficient O(1) vertical spatial relationship testing using precomputed bounding box information.

The function follows the same pattern as the horizontal comparison functions (`poly_left`, `poly_right`) but operates on the Y-axis instead of the X-axis. It includes proper memory management for toasted inputs, making it suitable for R-tree spatial indexing operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument: `POLYGON *polya` - the polygon to test if it's below
  - Second argument: `POLYGON *polyb` - the polygon to compare against

## Dependencies  
- Functions called/Symbols referenced:
  - `PG_GETARG_POLYGON_P` - extracts polygon arguments from function call
  - `PG_FREE_IF_COPY` - frees memory for toasted inputs
  - `PG_RETURN_BOOL` - returns boolean result
- Called from (representative examples):
  - No direct references found (likely used via SQL operator system)

## Notes and Other Information
- The function uses bounding box comparison for efficiency rather than examining all polygon vertices
- The comparison is strict (uses < rather than <=), meaning touching polygons are not considered "below" each other
- This operates on the Y-axis, complementing the X-axis operations of the left/right comparison functions
- Typically invoked through PostgreSQL's operator system (likely a vertical positioning operator for polygons)
- Essential for R-tree spatial indexing operations on polygon data
- Memory management carefully handles toasted polygon data to prevent leaks