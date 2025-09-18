# spg_range_quad_picksplit

## Location
src/backend/utils/adt/rangetypes_spgist.c: 200 - 299

## Overview
SP-GiST picksplit function that divides a collection of ranges into child nodes by selecting a centroid range and distributing ranges according to quadrants.

## Definition
Datum spg_range_quad_picksplit(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the node splitting logic for SP-GiST quadtree indexing of range types. When a node becomes full, it analyzes all ranges in the node to construct a "centroid" range based on median values of the lower and upper bounds. It then distributes the ranges into quadrant-based child nodes according to their spatial relationship with the centroid. The function handles special cases including all-empty ranges and root-level splitting, creating appropriate node structures for efficient quadtree navigation.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro for PostgreSQL function argument handling
- in: spgPickSplitIn structure containing ranges to split and node information
- out: spgPickSplitOut structure populated with splitting results
- nonEmptyCount: Count of non-empty ranges in the input set
- centroid: The calculated centroid range used for quadrant determination
- lowerBounds, upperBounds: Arrays of range bounds extracted from input ranges
- typcache: Type cache entry for range operations

## Dependencies
- Functions called/Symbols referenced:
  - spgPickSplitIn, spgPickSplitOut (structure types)
  - RangeBound (structure type)
  - range_get_typcache (type cache retrieval)
  - RangeTypeGetOid, DatumGetRangeTypeP (range type operations)
  - range_deserialize, range_serialize (range serialization)
  - palloc (memory allocation)
  - qsort_arg (sorting with context)
  - bound_cmp (comparison function for sorting)
  - getQuadrant (quadrant determination)
  - RangeTypePGetDatum (range to datum conversion)
  - PG_RETURN_VOID (macro)
- Called from (representative examples):
  - SP-GiST index splitting operations when nodes become full

## Notes and Other Information
- Constructs centroid from median lower and upper bounds of non-empty ranges
- Creates 2 nodes for all-empty case: node 0 for empty ranges, node 1 for future non-empty
- Creates 5 nodes at root level (including empty range node), 4 nodes otherwise
- Maps ranges to nodes based on quadrant (1-4 becomes nodes 0-3)
- Handles memory allocation for node mapping and leaf tuple storage
- Preserves original range data in leaf nodes for exact retrieval
- Essential for maintaining balanced quadtree structure during index growth
- Located in src/backend/utils/adt/rangetypes_spgist.c:200-299