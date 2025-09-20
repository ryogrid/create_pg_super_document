# poly_left

## Location
[src/backend/utils/adt/geo_ops.c:3533-3555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3533-L3555)

## Overview
Determines if polygon A is strictly to the left of polygon B by comparing their bounding box coordinates.

## Definition

```c
Datum
poly_left(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements a geometric comparison operator that checks whether one polygon is positioned strictly to the left of another polygon. It performs this check by comparing the bounding boxes of the two polygons - specifically, it returns true if the rightmost point (high.x) of polygon A is less than the leftmost point (low.x) of polygon B. This is an efficient O(1) operation that uses precomputed bounding box information rather than examining all vertices of the polygons.

The function is designed to work with PostgreSQL's geometric data types and follows the standard PostgreSQL function calling convention using . It includes proper memory management to avoid leaking memory for toasted (compressed) input values, which is particularly important for R-tree index operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument:  - the polygon to test if it's on the left
  - Second argument:  - the polygon to compare against

## Dependencies
- Functions called/Symbols referenced:
  -  - extracts polygon arguments from function call
  -  - frees memory for toasted inputs
  -  - returns boolean result
- Called from (representative examples):
  - No direct references found (likely used via SQL operator system)

## Notes and Other Information
- The function uses bounding box comparison for efficiency rather than detailed geometric analysis
- Memory management is carefully handled to prevent leaks with toasted (compressed) polygon data
- This is typically invoked through PostgreSQL's operator system (likely the << operator for polygons)
- The comparison is strict (uses < rather than <=), meaning touching polygons are not considered "left of" each other
- Essential for R-tree spatial indexing operations on polygon data