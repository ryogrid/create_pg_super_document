# poly_overleft

## Location
[src/backend/utils/adt/geo_ops.c:3556-3578](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3556-L3578)

## Overview
Determines if polygon A is overlapping with or positioned to the left of polygon B by comparing their rightmost bounding box coordinates.

## Definition
```c
Datum poly_overleft(PG_FUNCTION_ARGS)
```

## Detailed Description
The `poly_overleft` function implements a geometric comparison operator that checks whether one polygon is positioned to the left of or overlapping with another polygon. Unlike `poly_left` which requires strict separation, this function returns true if the rightmost point (high.x) of polygon A is less than or equal to the rightmost point (high.x) of polygon B. This allows for cases where the polygons overlap or touch along their boundaries.

The function uses bounding box comparison for efficiency and is commonly used in spatial indexing operations. It follows PostgreSQL's standard function calling conventions and includes proper memory management for toasted inputs.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument: `POLYGON *polya` - the polygon to test if it's overlapping/left
  - Second argument: `POLYGON *polyb` - the polygon to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POLYGON_P` - extracts polygon arguments from function call
  - `PG_FREE_IF_COPY` - frees memory for toasted inputs  
  - `PG_RETURN_BOOL` - returns boolean result
- Called from (representative examples):
  - No direct references found (likely used via SQL operator system)

## Notes and Other Information
- Uses <= comparison allowing for overlapping or touching polygons
- More permissive than `poly_left` which requires strict separation
- Essential for R-tree spatial indexing and geometric query optimization
- Typically invoked through PostgreSQL's operator system (likely the &< operator for polygons)
- Memory management prevents leaks with compressed polygon data

## Simplified Source

```c
Datum poly_overleft(PG_FUNCTION_ARGS) {
    // Extract both polygons from function arguments
    POLYGON *polya = PG_GETARG_POLYGON_P(0);
    POLYGON *polyb = PG_GETARG_POLYGON_P(1);

    // Check if polygon A overlaps or is left of polygon B
    // Compare rightmost points (allows for overlap/touching)
    bool result = polya->boundbox.high.x <= polyb->boundbox.high.x;

    // Free memory for toasted inputs (important for R-tree indexes)
    PG_FREE_IF_COPY(polya, 0);
    PG_FREE_IF_COPY(polyb, 1);

    PG_RETURN_BOOL(result);
}
```