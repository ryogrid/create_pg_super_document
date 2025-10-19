# box_poly

## Location
[src/backend/utils/adt/geo_ops.c:4535-4563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4535-L4563)

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
  - [palloc](../p/palloc.md) (memory allocation)
  - SET_VARSIZE (macro to set variable-length type size)
  - [box_construct](box_construct.md) (constructs bounding box from two points)
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

## Simplified Source

```c
Datum
box_poly(PG_FUNCTION_ARGS)
{
    BOX *box = PG_GETARG_BOX_P(0);
    POLYGON *poly;
    int size;

    // Allocate memory for polygon with 4 vertices (box corners)
    size = offsetof(POLYGON, p) + sizeof(poly->p[0]) * 4;
    poly = (POLYGON *) palloc(size);

    // Set polygon properties
    SET_VARSIZE(poly, size);
    poly->npts = 4;

    // Map box corners to polygon vertices (counter-clockwise)
    poly->p[0].x = box->low.x;   // Lower-left
    poly->p[0].y = box->low.y;
    poly->p[1].x = box->low.x;   // Upper-left
    poly->p[1].y = box->high.y;
    poly->p[2].x = box->high.x;  // Upper-right
    poly->p[2].y = box->high.y;
    poly->p[3].x = box->high.x;  // Lower-right
    poly->p[3].y = box->low.y;

    // Set bounding box (same as original box)
    box_construct(&poly->boundbox, &box->high, &box->low);

    PG_RETURN_POLYGON_P(poly);
}
```