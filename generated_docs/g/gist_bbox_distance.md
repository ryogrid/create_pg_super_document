# gist_bbox_distance

## Location
src/backend/access/gist/gistproc.c: 1479 - 1499

## Overview
A static utility function that calculates the minimum distance from a query point to a bounding box stored in a GiST entry, serving as a helper for various geometric distance methods.

## Definition
```c
static float8 gist_bbox_distance(GISTENTRY *entry, Datum query, StrategyNumber strategy)
```

## Detailed Description
This internal utility function provides distance calculation between a query point and a bounding box represented by a GiST entry. It serves as a common helper function used by multiple geometric distance methods (`gist_box_distance`, `gist_circle_distance`, `gist_poly_distance`) to compute distances from points to various geometric objects.

The function categorizes queries by strategy group and currently supports only point-to-bounding-box distance calculations. It uses the `computeDistance` utility with `false` as the first parameter, indicating that it should always treat the entry as a bounding box (not as a leaf point), regardless of the actual entry type.

This approach provides consistent distance computation for geometric objects that are indexed using their bounding boxes in GiST indexes.

## Parameters / Member Variables
- `entry`: GiST entry containing the bounding box data to calculate distance to
- `query`: Query point (as Datum) to calculate distance from  
- `strategy`: Strategy number indicating the type of distance operation
- Returns: Distance as a float8 value representing the minimum distance from the query point to the bounding box

## Dependencies
- Functions called/Symbols referenced:
  - `computeDistance`
  - `DatumGetBoxP`
  - `DatumGetPointP`
  - `GeoStrategyNumberOffset`
  - `PointStrategyNumberGroup`
- Called from (representative examples):
  - `gist_box_distance`
  - `gist_circle_distance`
  - `gist_poly_distance`

## Notes and Other Information
- Static function with internal linkage, not exposed outside gistproc.c
- Always calls `computeDistance` with `false` first parameter to force bounding box distance calculation
- Currently only supports `PointStrategyNumberGroup` queries; other strategy groups result in an error  
- Provides a consistent interface for point-to-bounding-box distance calculations across different geometric types
- Essential for KNN searches involving geometric objects that are indexed by their bounding boxes