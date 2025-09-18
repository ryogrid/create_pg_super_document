# poly_right

## Location
[src/backend/utils/adt/geo_ops.c:3579-3601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3579-L3601)

## Overview  
Determines if polygon A is strictly to the right of polygon B by comparing their bounding box coordinates.

## Definition
```c
Datum poly_right(PG_FUNCTION_ARGS)
```

## Detailed Description
The `poly_right` function implements a geometric comparison operator that checks whether one polygon is positioned strictly to the right of another polygon. It performs this check by comparing the bounding boxes of the two polygons - specifically, it returns true if the leftmost point (low.x) of polygon A is greater than the rightmost point (high.x) of polygon B. This is the mirror operation to `poly_left`, providing efficient O(1) spatial relationship testing.

The function follows PostgreSQL's standard function calling conventions and includes proper memory management to avoid leaking memory for toasted (compressed) input values, which is essential for R-tree index operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument: `POLYGON *polya` - the polygon to test if it's on the right
  - Second argument: `POLYGON *polyb` - the polygon to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POLYGON_P` - extracts polygon arguments from function call
  - `PG_FREE_IF_COPY` - frees memory for toasted inputs
  - `PG_RETURN_BOOL` - returns boolean result
- Called from (representative examples):
  - No direct references found (likely used via SQL operator system)

## Notes and Other Information
- The function uses bounding box comparison for efficiency rather than detailed geometric analysis
- The comparison is strict (uses > rather than >=), meaning touching polygons are not considered "right of" each other
- This is the complementary operation to `poly_left`, testing rightward spatial relationships
- Typically invoked through PostgreSQL's operator system (likely the >> operator for polygons)  
- Essential for R-tree spatial indexing operations on polygon data
- Memory management carefully handles toasted polygon data to prevent leaks