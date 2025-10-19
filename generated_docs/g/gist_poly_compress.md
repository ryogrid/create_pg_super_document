# gist_poly_compress

## Location
[src/backend/access/gist/gistproc.c:1035-1061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L1035-L1061)

## Overview
Compresses polygon data for GiST index storage by extracting and storing only the bounding box representation of the polygon.

## Definition
```c
Datum gist_poly_compress(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the compress method for polygon data types in PostgreSQL's GiST (Generalized Search Tree) access method. The compression strategy for polygons involves representing the complex polygon geometry with its simpler bounding box (BOX) for efficient spatial indexing.

For leaf entries containing actual polygon data, the function extracts the pre-computed bounding box from the polygon's boundbox field and creates a new GISTENTRY containing just this bounding box. For non-leaf entries that already contain compressed data (bounding boxes), the function simply returns the entry unchanged.

This compression allows the GiST index to perform spatial operations efficiently using simple box comparisons while still maintaining the ability to access the original polygon data when needed.

## Parameters / Member Variables
- `entry`: The GISTENTRY pointer containing either a polygon (for leaf nodes) or a bounding box (for internal nodes)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (to extract function arguments)
  - [DatumGetPolygonP](../D/DatumGetPolygonP.md) (to convert Datum to POLYGON pointer)
  - [palloc](../p/palloc.md) (PostgreSQL's memory allocation)
  - memcpy (to copy bounding box data)
  - gistentryinit (to initialize new GISTENTRY)
  - [PointerGetDatum](../P/PointerGetDatum.md) (to convert pointer to Datum)
  - PG_RETURN_POINTER (to return result)
- Called from (representative examples):
  - GiST index access method framework (via function registration)

## Notes and Other Information
- This function is registered as the compress method for polygon GiST operator classes
- Only compresses leaf entries; internal entries are already in compressed (bounding box) form
- Uses the pre-computed boundbox field from the POLYGON structure for efficiency
- The compression is lossy in the sense that spatial operations work on bounding boxes rather than exact polygon geometry
- Memory for the new bounding box and GISTENTRY is allocated in the current memory context
- Part of PostgreSQL's spatial indexing infrastructure for efficient polygon queries

## Simplified Source

```c
Datum gist_poly_compress(PG_FUNCTION_ARGS) {
    GISTENTRY *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    GISTENTRY *retval;

    if (entry->leafkey) {
        // Extract polygon and get its bounding box
        POLYGON *polygon = DatumGetPolygonP(entry->key);
        BOX *boundbox = (BOX *) palloc(sizeof(BOX));

        // Copy the pre-computed bounding box from polygon
        memcpy(boundbox, &(polygon->boundbox), sizeof(BOX));

        // Create new entry with bounding box instead of full polygon
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