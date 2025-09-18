# spg_range_quad_choose

## Location
src/backend/utils/adt/rangetypes_spgist.c: 131 - 185

## Overview
SP-GiST choose function that determines the appropriate child node path for inserting a new range into the quadtree index structure.

## Definition
Datum spg_range_quad_choose(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the choose logic for SP-GiST quadtree indexing of range types. When inserting a new range, it determines which child node the range should be routed to based on the current node's structure. For nodes without a centroid, it separates ranges based on emptiness (empty ranges go to node 0, non-empty to node 1). For nodes with a centroid range, it uses the getQuadrant function to determine which quadrant the new range falls into relative to the centroid, then routes it to the corresponding child node.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro for PostgreSQL function argument handling
- in: spgChooseIn structure containing input data including the range to insert and node information
- out: spgChooseOut structure that gets populated with routing decisions
- inRange: The range type being inserted into the index
- centroid: The centroid range of the current node (if present)
- quadrant: The quadrant number (1-4) determined for the input range
- typcache: Type cache entry for range operations

## Dependencies
- Functions called/Symbols referenced:
  - [spgChooseIn](spgChooseIn.md), spgChooseOut (structure types)
  - DatumGetRangeTypeP, RangeTypePGetDatum (range conversion functions)
  - [range_get_typcache](../r/range_get_typcache.md) (type cache retrieval)
  - RangeTypeGetOid (range type OID extraction)
  - RangeIsEmpty (empty range test)
  - [getQuadrant](../g/getQuadrant.md) (quadrant determination)
  - spgMatchNode (enum value)
  - PG_RETURN_VOID (macro)
- Called from (representative examples):
  - SP-GiST index insertion operations

## Notes and Other Information
- Handles three scenarios: allTheSame nodes, nodes without centroid, and nodes with centroid
- Empty ranges are always routed to child node 0 when no centroid exists
- Non-empty ranges go to child node 1 when no centroid exists
- With centroid, uses quadrant-based routing (quadrant 1-4 maps to nodes 0-3)
- Sets levelAdd to indicate tree level progression
- Preserves original range data in restDatum for leaf storage
- Located in src/backend/utils/adt/rangetypes_spgist.c:131-185