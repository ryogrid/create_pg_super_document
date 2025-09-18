# gist_circle_distance

## Location
src/backend/access/gist/gistproc.c: 1526 - 1542

## Overview
Implements the GiST distance method for circle data types, providing inexact distance calculations from query points to circles by computing distances to their bounding boxes for efficient nearest-neighbor searches.

## Definition
```c
Datum gist_circle_distance(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the GiST distance calculation method for circle data types in k-nearest neighbor (KNN) searches. As part of the "inexact GiST distance methods for geometric types," it provides a lower bound estimate of the actual distance by calculating the distance from the query point to the minimum bounding rectangle (MBR) of the indexed circle.

The function delegates the core distance computation to `gist_bbox_distance`, which computes the distance from the query point to the bounding box stored in the GiST entry. Since this is only an approximation (the actual circle may be smaller than its bounding box), the function sets `*recheck = true` to indicate that the result requires refinement at higher levels of the query processing.

This approach provides efficient pruning during KNN searches while maintaining correctness through the recheck mechanism.

## Parameters / Member Variables
- `entry`: GiST entry containing the indexed circle's bounding box information
- `query`: Query datum (typically a point) to calculate distance from
- `strategy`: Strategy number indicating the type of distance operation
- `subtype`: OID parameter (currently unused, commented out)
- `recheck`: Output parameter set to true to indicate the result needs verification
- Returns: Distance as a float8 value representing the lower bound distance estimate

## Dependencies
- Functions called/Symbols referenced:
  - [gist_bbox_distance](gist_bbox_distance.md)
  - `PG_GETARG_POINTER`
  - `PG_GETARG_DATUM`
  - `PG_GETARG_UINT16`
  - `PG_RETURN_FLOAT8`
- Called from (representative examples):
  - GiST index access methods during KNN searches on circle data (indirectly through function pointers)

## Notes and Other Information
- Part of the inexact distance methods that provide lower bound estimates for efficient KNN searches
- Always sets `*recheck = true` because bounding box distance is only an approximation of actual circle distance
- Uses the same bounding box distance logic as other geometric types through `gist_bbox_distance`
- Essential for ORDER BY distance queries on circle data types in GiST indexes
- The inexact nature allows for efficient pruning while maintaining correctness through rechecking