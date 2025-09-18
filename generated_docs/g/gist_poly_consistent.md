# gist_poly_consistent

## Location
src/backend/access/gist/gistproc.c: 1062 - 1099

## Overview
Implements the consistent method for polygon data types in GiST indexes by performing bounding box-based spatial consistency checks that require revalidation against actual polygon geometry.

## Definition
```c
Datum gist_poly_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the consistent method for polygon data types in PostgreSQL's GiST access method. It determines whether a polygon query could potentially match entries in a GiST index subtree by comparing bounding boxes rather than exact polygon geometry.

The function extracts the query polygon and the index entry (which is a bounding box due to compression), then delegates the actual spatial logic to rtree_internal_consistent. Since bounding box comparisons are approximations of actual polygon spatial relationships, all cases require recheck - meaning the exact polygon-to-polygon comparison must be performed later to confirm the match.

The function uses the same internal consistency logic for both leaf and internal nodes because index entries are stored as bounding boxes at all levels after compression. This design allows for consistent spatial filtering while deferring expensive exact polygon computations until recheck time.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS macro to access function arguments:
  - `entry`: GISTENTRY pointer containing the index key (bounding box)
  - `query`: POLYGON pointer representing the query polygon
  - `strategy`: StrategyNumber indicating the spatial operation type
  - `recheck`: Boolean pointer to indicate whether recheck is required (always set to true)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER, PG_GETARG_POLYGON_P, PG_GETARG_UINT16 (argument extraction)
  - DatumGetBoxP (to extract bounding box from index entry)
  - rtree_internal_consistent (for actual spatial logic)
  - PG_FREE_IF_COPY (to avoid memory leaks with toasted data)
  - PG_RETURN_BOOL (to return result)
- Called from (representative examples):
  - GiST index access method framework
  - gist_point_consistent (for point-in-polygon queries)

## Notes and Other Information
- Always sets recheck to true since bounding box comparisons are inexact for polygons
- Uses rtree_internal_consistent for both leaf and internal nodes due to compressed representation
- Handles NULL checks for both index entry and query polygon
- Properly manages memory for toasted (compressed) polygon data to prevent leaks
- Part of PostgreSQL's spatial indexing infrastructure enabling efficient polygon queries
- The approximation nature allows fast spatial filtering with exact verification deferred to recheck phase