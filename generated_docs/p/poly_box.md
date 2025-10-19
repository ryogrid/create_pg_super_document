# poly_box

## Location
[src/backend/utils/adt/geo_ops.c:4519-4534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4519-L4534)

## Overview
Extracts and returns the bounding box of a polygon as a separate BOX geometric type.

## Definition
```c
Datum poly_box(PG_FUNCTION_ARGS)
```

## Detailed Description
The `poly_box` function extracts the bounding box from a polygon and returns it as a standalone BOX geometric type. The function accesses the pre-computed `boundbox` field from the POLYGON structure, which contains the minimum and maximum x and y coordinates that encompass all vertices of the polygon. This provides an efficient way to obtain the rectangular boundary that contains the entire polygon.

## Parameters / Member Variables
- Input: A POLYGON pointer obtained via `PG_GETARG_POLYGON_P(0)` - the polygon whose bounding box is requested
- Returns: A BOX pointer via `PG_RETURN_BOX_P()` - the bounding box of the polygon

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P (macro to extract POLYGON argument)
  - [palloc](palloc.md) (memory allocation for result BOX)
  - PG_RETURN_BOX_P (macro to return BOX result)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- The function relies on the `boundbox` field being pre-computed in the POLYGON structure
- This is a simple accessor function that performs a direct copy of the bounding box data
- Memory is allocated for the result BOX structure using palloc()
- The bounding box is typically computed when the polygon is created or modified
- Useful for spatial indexing and quick geometric comparisons
- Located in src/backend/utils/adt/geo_ops.c:4519-4534

## Simplified Source

```c
Datum
poly_box(PG_FUNCTION_ARGS)
{
    POLYGON *poly = PG_GETARG_POLYGON_P(0);
    BOX *box;

    // Allocate memory for result box
    box = (BOX *) palloc(sizeof(BOX));

    // Copy the pre-computed bounding box from polygon
    *box = poly->boundbox;

    PG_RETURN_BOX_P(box);
}
```