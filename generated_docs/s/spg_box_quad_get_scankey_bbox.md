# spg_box_quad_get_scankey_bbox

## Location
[src/backend/utils/adt/geo_spgist.c:531-552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L531-L552)

## Overview
A static helper function that extracts a bounding box from a scan key for SP-GiST spatial queries, handling different geometric data types and setting recheck flags as needed.

## Definition
```c
static BOX *spg_box_quad_get_scankey_bbox(ScanKey sk, bool *recheck)
```

## Detailed Description
This function serves as a unified interface for obtaining bounding box representations from scan keys in SP-GiST spatial queries. It handles multiple geometric data types by examining the scan key's subtype and extracting the appropriate bounding box information. The function also manages the recheck mechanism, which determines whether exact geometric validation is required after the initial bounding box test.

The function supports two primary geometric types:
1. BOX objects: Direct return of the box data
2. POLYGON objects: Extraction of the polygon's precomputed bounding box with potential recheck flagging

For polygon queries, the function uses the is_bounding_box_test_exact helper to determine if the bounding box test provides definitive results or if a recheck of the actual geometric relationship is required.

## Parameters / Member Variables
- `sk` (ScanKey): The scan key containing the query geometry and strategy information
- `recheck` (bool*): Output parameter set to true if exact geometric validation is required after bounding box filtering

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetBoxP](../D/DatumGetBoxP.md) (converts Datum to BOX pointer)
  - [DatumGetPolygonP](../D/DatumGetPolygonP.md) (converts Datum to POLYGON pointer)
  - [is_bounding_box_test_exact](../i/is_bounding_box_test_exact.md) (determines if bbox test is sufficient)
  - elog (PostgreSQL error logging function)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](spg_box_quad_inner_consistent.md) (consistency checking for internal nodes)
  - [spg_box_quad_leaf_consistent](spg_box_quad_leaf_consistent.md) (consistency checking for leaf nodes)

## Notes and Other Information
- Static function with internal linkage, only used within geo_spgist.c
- Handles BOXOID and POLYGONOID geometry types specifically
- Throws an ERROR for unrecognized scan key subtypes
- Uses precomputed bounding boxes from polygon structures for efficiency
- The recheck mechanism optimizes query performance by avoiding unnecessary exact calculations
- Located in src/backend/utils/adt/geo_spgist.c:531-552
- Part of PostgreSQL's SP-GiST spatial indexing infrastructure