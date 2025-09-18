# spg_range_quad_inner_consistent

## Location
src/backend/utils/adt/rangetypes_spgist.c: 300 - 784

## Overview
SP-GiST inner node consistent function for range types that determines which child nodes should be visited during index traversal based on query conditions.

## Definition


## Detailed Description
This function implements the inner node consistent logic for SP-GiST (Space-Partitioned Generalized Search Tree) indexing of PostgreSQL range types. It analyzes query conditions against the current inner node's centroid to determine which child quadrants need to be visited during index traversal.

The function handles two main cases:
1. **Non-centroid nodes**: Inner nodes without a centroid have exactly 2 child nodes - one for empty ranges and one for non-empty ranges
2. **Centroid nodes**: Inner nodes with a centroid partition the space into 4 or 5 quadrants based on the relationship between ranges and the centroid

For centroid nodes, the quadrants represent:
- Quadrant 1: Ranges with lower bound ≥ centroid upper bound, upper bound ≥ centroid upper bound  
- Quadrant 2: Ranges with lower bound ≤ centroid upper bound, upper bound ≥ centroid upper bound
- Quadrant 3: Ranges with lower bound ≤ centroid upper bound, upper bound ≤ centroid upper bound
- Quadrant 4: Ranges with lower bound ≥ centroid lower bound, upper bound ≤ centroid upper bound
- Quadrant 5: Empty ranges (if present)

The function processes each scan key strategy (BEFORE, OVERLEFT, OVERLAPS, OVERRIGHT, AFTER, ADJACENT, CONTAINS, CONTAINED_BY, EQ, CONTAINS_ELEM) to determine which quadrants could possibly contain matching ranges.

## Parameters / Member Variables
- : Input structure containing scan keys, node information, centroid data, and traversal context
- : Output structure where selected child node numbers and traversal values are stored

## Dependencies
- Functions called/Symbols referenced:
  - RangeIsEmpty
  - DatumGetRangeTypeP  
  - [range_get_typcache](../r/range_get_typcache.md)
  - RangeTypeGetOid
  - [range_deserialize](../r/range_deserialize.md)
  - [adjacent_inner_consistent](../a/adjacent_inner_consistent.md)
  - [getQuadrant](../g/getQuadrant.md)
  - [range_cmp_bounds](../r/range_cmp_bounds.md)
  - [datumCopy](../d/datumCopy.md)
- Called from (representative examples):
  - No direct references found (likely called through function pointer in SP-GiST operator class)

## Notes and Other Information
- For ADJACENT strategy, the function uses  to improve precision by considering previous centroid information
- The function sets  for adjacent searches to pass previous centroid data to child nodes
- Memory for traversal values is allocated in the traversal memory context to persist across index operations
- The 'which' bitmask tracks which child nodes should be visited, with bit N corresponding to child node N-1
- Special handling for  case where all tuples have identical centroid values