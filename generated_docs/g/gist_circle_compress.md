# gist_circle_compress

## Location
[src/backend/access/gist/gistproc.c:1100-1129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L1100-L1129)

## Overview
Compresses circle data for GiST index storage by computing and storing the bounding box representation of the circle's spatial extent.

## Definition
```c
Datum gist_circle_compress(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the compress method for circle data types in PostgreSQL's GiST (Generalized Search Tree) access method. The compression strategy for circles involves converting the circle geometry (center point and radius) into its bounding box representation for efficient spatial indexing.

For leaf entries containing actual circle data, the function calculates the bounding box by determining the minimum and maximum x and y coordinates based on the circle's center point and radius. The bounding box coordinates are computed as:
- high.x = center.x + radius  
- low.x = center.x - radius
- high.y = center.y + radius
- low.y = center.y - radius

For non-leaf entries that already contain compressed data (bounding boxes), the function returns the entry unchanged. This compression enables the GiST index to perform spatial operations efficiently using simple box comparisons while maintaining access to original circle data when needed.

## Parameters / Member Variables
- `entry`: The GISTENTRY pointer containing either a circle (for leaf nodes) or a bounding box (for internal nodes)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (to extract function arguments)
  - [DatumGetCircleP](../D/DatumGetCircleP.md) (to convert Datum to CIRCLE pointer)
  - [palloc](../p/palloc.md) (PostgreSQL's memory allocation)
  - [float8_pl](../f/float8_pl.md), float8_mi (floating-point addition and subtraction)
  - gistentryinit (to initialize new GISTENTRY)
  - [PointerGetDatum](../P/PointerGetDatum.md) (to convert pointer to Datum)
  - PG_RETURN_POINTER (to return result)
- Called from (representative examples):
  - GiST index access method framework (via function registration)

## Notes and Other Information
- This function is registered as the compress method for circle GiST operator classes
- Only compresses leaf entries; internal entries are already in compressed (bounding box) form
- Manually calculates bounding box coordinates using the circle's center and radius
- The compression is lossy since spatial operations work on bounding boxes rather than exact circle geometry
- Uses PostgreSQL's float8_pl and float8_mi functions for safe floating-point arithmetic
- Memory for the new bounding box and GISTENTRY is allocated in the current memory context
- Part of PostgreSQL's spatial indexing infrastructure enabling efficient circle-based spatial queries

## Simplified Source

```c
Datum gist_circle_compress(PG_FUNCTION_ARGS) {
    GISTENTRY *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    GISTENTRY *retval;

    if (entry->leafkey) {
        // Extract circle and compute its bounding box
        CIRCLE *circle = DatumGetCircleP(entry->key);
        BOX *boundbox = (BOX *) palloc(sizeof(BOX));

        // Calculate bounding box from center and radius
        boundbox->high.x = float8_pl(circle->center.x, circle->radius);
        boundbox->low.x = float8_mi(circle->center.x, circle->radius);
        boundbox->high.y = float8_pl(circle->center.y, circle->radius);
        boundbox->low.y = float8_mi(circle->center.y, circle->radius);

        // Create new entry with bounding box instead of full circle
        retval = (GISTENTRY *) palloc(sizeof(GISTENTRY));
        gistentryinit(*retval, PointerGetDatum(boundbox),
                      entry->rel, entry->page, entry->offset, false);
    } else {
        // Internal nodes already contain compressed data
        retval = entry;
    }

    PG_RETURN_POINTER(retval);
}
```