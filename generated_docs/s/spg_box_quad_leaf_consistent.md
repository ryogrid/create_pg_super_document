# spg_box_quad_leaf_consistent

## Location
src/backend/utils/adt/geo_spgist.c: 741 - 858

## Overview
A leaf consistency function for SP-GiST quadtree indexes on geometric box and polygon data types that determines whether a leaf node matches the search criteria.

## Definition


## Detailed Description
This function implements the leaf consistency check for SP-GiST quadtree indexes on box and polygon geometries. It evaluates whether a stored leaf value satisfies the given search conditions by testing various spatial relationships (overlap, containment, position, etc.). The function supports all standard R-tree strategy numbers for 2D spatial queries and can handle distance-based ordering for nearest neighbor searches. All tests performed are exact matches with no need for rechecking, except when computing distances to polygons.

## Parameters / Member Variables
- : spgLeafConsistentIn structure containing:
  - : The stored leaf value to test
  - : Array of search conditions to evaluate
  - : Number of search keys
  - : Array of ordering conditions for distance queries
  - : Number of ordering keys
  - : Flag indicating whether to return the leaf value
- : spgLeafConsistentOut structure for results:
  - : Set to false (exact tests)
  - : Returned leaf datum if requested
  - : Computed distances for ordering
  - : Flag for distance recheck requirement

## Dependencies
- Functions called/Symbols referenced:
  - [spg_box_quad_get_scankey_bbox](spg_box_quad_get_scankey_bbox.md)
  - [BoxPGetDatum](../B/BoxPGetDatum.md)
  - DirectFunctionCall2
  - [box_overlap](../b/box_overlap.md), box_contain, box_contained, box_same
  - [box_left](../b/box_left.md), box_overleft, box_right, box_overright
  - [box_above](../b/box_above.md), box_overabove, box_below, box_overbelow
  - [spg_key_orderbys_distances](spg_key_orderbys_distances.md)
- Called from (representative examples):
  - Used as SP-GiST leaf consistent function in operator class definitions

## Notes and Other Information
- Supports all R-tree strategy numbers for comprehensive spatial querying
- Works with both box and polygon operator classes, though leaf data handling differs
- Distance calculations require rechecking when the distance function is F_DIST_POLYP (polygon distance)
- Returns early on first failed condition for efficiency
- Part of PostgreSQL's SP-GiST framework for spatial indexing