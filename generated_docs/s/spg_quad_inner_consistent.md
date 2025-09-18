# spg_quad_inner_consistent

## Location
src/backend/access/spgist/spgquadtreeproc.c: 227 - 406

## Overview
An SP-GiST operator function that determines which child nodes should be visited during quadtree traversal based on query constraints, implementing the inner_consistent operation for spatial index queries.

## Definition


## Detailed Description
The  function is a critical component of PostgreSQL's SP-GiST quadtree implementation that determines which child nodes to visit during index traversal. The function:

1. **Analyzes query constraints**: Processes scan keys to determine which quadrants satisfy the query conditions
2. **Handles various spatial strategies**: Supports multiple spatial query operations including left/right, above/below, same position, and containment
3. **Manages bounding boxes**: When order-by clauses are present, calculates and maintains bounding box information for distance calculations
4. **Optimizes traversal**: Uses bit masking to efficiently determine which of the four quadrants need to be visited
5. **Handles special cases**: Processes the "allTheSame" condition where all tuples are identical

The function uses a bitmask approach where each bit represents one of the four quadrants (NE, NW, SE, SW), and query constraints progressively narrow down which quadrants satisfy all conditions.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (spgInnerConsistentIn*): Input structure with scan keys, prefix datum, and traversal state
  -  (spgInnerConsistentOut*): Output structure populated with nodes to visit and distance information

## Dependencies
- Functions called/Symbols referenced:
  - [spgInnerConsistentIn](spgInnerConsistentIn.md)/spgInnerConsistentOut
  - [Point](../P/Point.md), BOX
  - [DatumGetPointP](../D/DatumGetPointP.md), DatumGetBoxP
  - get_float8_infinity
  - [getQuadrant](../g/getQuadrant.md), getQuadrantArea
  - [box_copy](../b/box_copy.md), box_contain_pt
  - [spg_key_orderbys_distances](spg_key_orderbys_distances.md)
  - SPTEST macro with point comparison functions (point_left, point_right, point_above, point_below)
  - Strategy number constants (RTLeftStrategyNumber, RTRightStrategyNumber, etc.)
  - [BoxPGetDatum](../B/BoxPGetDatum.md), PG_RETURN_VOID
- Called from (representative examples):
  - SP-GiST framework (via function pointers in operator class)

## Notes and Other Information
- The function assumes exactly 4 child nodes representing the four quadrants of 2D space
- Uses bit manipulation for efficient quadrant filtering:  variable tracks valid quadrants as a bitmask
- Handles distance calculations for ORDER BY queries by maintaining bounding box information in traversalValues
- Supports both point and box query types depending on the strategy number
- The RTContainedByStrategyNumber case specifically handles box containment queries by checking all four corners
- Memory management uses the traversal memory context for persistent bounding box storage
- Part of PostgreSQL's extensible indexing framework for spatial data types
- Optimizes early termination when no quadrants satisfy the constraints (which == 0)