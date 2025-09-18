# spg_kd_inner_consistent

## Location
src/backend/access/spgist/spgkdtreeproc.c: 160 - 349

## Overview
A SP-GiST inner consistent function that determines which child nodes to traverse during k-d tree searches by evaluating query constraints against the splitting coordinate at each internal node.

## Definition
```c
Datum spg_kd_inner_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the inner_consistent operation for SP-GiST k-d tree indexes. It evaluates search predicates at internal tree nodes to determine which child nodes need to be traversed to satisfy the query. The function handles various geometric query strategies including point location, containment, and spatial relationships.

The function processes each scan key constraint and uses a bitmask approach to track which children (left=1, right=2) satisfy all conditions. It alternates between X and Y coordinate comparisons based on the tree level, matching the splitting strategy used by spg_kd_picksplit.

For ordered searches (nearest neighbor queries), the function calculates bounding boxes for child nodes and computes distances to support distance-based traversal ordering.

Key features:
- Handles multiple query strategies (left/right, above/below, same point, containment)
- Alternates coordinate comparison based on tree level (X on odd levels, Y on even levels)
- Supports distance calculations for nearest neighbor searches
- Uses bitmask logic to efficiently determine which children to visit

## Parameters / Member Variables
- `in`: Input structure containing query information and context
  - `in->hasPrefix`: Indicates presence of splitting coordinate (always true for k-d trees)
  - `in->prefixDatum`: The splitting coordinate value for this node
  - `in->nNodes`: Number of child nodes (always 2 for k-d trees)
  - `in->level`: Current tree level (determines split dimension)
  - `in->nkeys`: Number of scan key constraints
  - `in->scankeys[]`: Array of query predicates to evaluate
  - `in->norderbys`: Number of distance-based ordering constraints
  - `in->allTheSame`: Should never be true for k-d trees
- `out`: Output structure populated with traversal decisions
  - `out->nNodes`: Number of child nodes to visit
  - `out->nodeNumbers[]`: Array of child node indices to traverse
  - `out->distances[]`: Distance values for ordered searches
  - `out->traversalValues[]`: Bounding box information for child nodes
  - `out->levelAdds[]`: Level increments for each child (always 1)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetFloat8
  - DatumGetPointP
  - DatumGetBoxP
  - FPlt, FPgt (floating point comparisons)
  - get_float8_infinity
  - box_copy
  - spg_key_orderbys_distances
  - BoxPGetDatum
  - palloc
  - MemoryContextSwitchTo
  - elog, Assert
  - PG_RETURN_VOID
- Strategy number constants:
  - RTLeftStrategyNumber, RTRightStrategyNumber
  - RTSameStrategyNumber
  - RTBelowStrategyNumber, RTOldBelowStrategyNumber
  - RTAboveStrategyNumber, RTOldAboveStrategyNumber
  - RTContainedByStrategyNumber
- Called from (representative examples):
  - SP-GiST index scanning operations (no direct references found in codebase)

## Notes and Other Information
- This function is part of the SP-GiST k-d tree operator class for geometric point data
- The bitmask approach efficiently tracks which children satisfy all query constraints
- Supports both exact match and range query operations on 2D point data
- Distance calculations for nearest neighbor queries use bounding box approximations
- The function will error if allTheSame is encountered, as this should not occur in properly balanced k-d trees
- Handles the RTContainedByStrategyNumber case where the query is a box rather than a point
- Located in src/backend/access/spgist/spgkdtreeproc.c:160-349