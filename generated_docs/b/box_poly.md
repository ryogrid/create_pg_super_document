# box_poly

## Location
src/backend/utils/adt/geo_ops.c: 4535 - 4563

## Overview
Converts a rectangular box to a polygon by creating a 4-vertex polygon representing the box's corners.

## Definition
```c
Datum box_poly(PG_FUNCTION_ARGS)
```

## Detailed Description
The `box_poly` function converts a BOX geometric type to a POLYGON by mapping the four corners of the box to polygon vertices. The function creates a 4-point polygon where the vertices are ordered counter-clockwise starting from the lower-left corner: (low.x, low.y), (low.x, high.y), (high.x, high.y), (high.x, low.y). After setting up the vertices, it constructs the bounding box for the polygon using the original box coordinates.

## Parameters / Member Variables
- Input: A BOX pointer obtained via `PG_GETARG_BOX_P(0)` - the box to be converted
- Returns: A POLYGON pointer via `PG_RETURN_POLYGON_P()` - the resulting 4-vertex polygon

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro to extract BOX argument)
  - palloc (memory allocation)
  - SET_VARSIZE (macro to set variable-length type size)
  - box_construct (constructs bounding box from two points)
  - PG_RETURN_POLYGON_P (macro to return POLYGON result)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- Always creates exactly 4 vertices representing the rectangle's corners
- Vertices are ordered counter-clockwise starting from lower-left corner
- Memory allocation is calculated as `offsetof(POLYGON, p) + sizeof(poly->p[0]) * 4`
- The bounding box of the resulting polygon is identical to the original box
- Provides conversion between two different representations of rectangular shapes
- Located in src/backend/utils/adt/geo_ops.c:4535-4563