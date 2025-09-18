# gist_poly_compress

## Location
src/backend/access/gist/gistproc.c: 1035 - 1061

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
- Uses PostgreSQL's PG_FUNCTION_ARGS macro to access function arguments
- `entry`: The GISTENTRY pointer containing either a polygon (for leaf nodes) or a bounding box (for internal nodes)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (to extract function arguments)
  - DatumGetPolygonP (to convert Datum to POLYGON pointer)
  - palloc (PostgreSQL's memory allocation)
  - memcpy (to copy bounding box data)
  - gistentryinit (to initialize new GISTENTRY)
  - PointerGetDatum (to convert pointer to Datum)
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