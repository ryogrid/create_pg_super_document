# spg_quad_inner_consistent

## Location
[src/backend/access/spgist/spgquadtreeproc.c:227-406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgquadtreeproc.c#L227-L406)

## Overview
An SP-GiST operator function that determines which child nodes should be visited during quadtree traversal based on query constraints, implementing the inner_consistent operation for spatial index queries.

## Definition

```c
Datum
spg_quad_inner_consistent(PG_FUNCTION_ARGS)
```
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
  - [get_float8_infinity](../g/get_float8_infinity.md)
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

## Simplified Source

```c
Datum
spg_quad_inner_consistent(PG_FUNCTION_ARGS)
{
    spgInnerConsistentIn *in = (spgInnerConsistentIn *) PG_GETARG_POINTER(0);
    spgInnerConsistentOut *out = (spgInnerConsistentOut *) PG_GETARG_POINTER(1);
    Point *centroid = DatumGetPointP(in->prefixDatum);
    BOX *bbox = NULL;
    int which;

    // Setup bounding box for distance calculations if needed
    if (in->norderbys > 0) {
        out->distances = palloc(sizeof(double *) * in->nNodes);
        out->traversalValues = palloc(sizeof(void *) * in->nNodes);

        if (in->level == 0) {
            // Create infinite bounding box for root level
            static BOX infbbox;
            double inf = get_float8_infinity();
            infbbox.high.x = inf; infbbox.high.y = inf;
            infbbox.low.x = -inf; infbbox.low.y = -inf;
            bbox = &infbbox;
        } else {
            bbox = in->traversalValue;
        }
    }

    // Handle "all the same" case - visit all nodes
    if (in->allTheSame) {
        out->nNodes = in->nNodes;
        out->nodeNumbers = palloc(sizeof(int) * in->nNodes);

        for (int i = 0; i < in->nNodes; i++) {
            out->nodeNumbers[i] = i;

            if (in->norderbys > 0) {
                MemoryContext oldCtx = MemoryContextSwitchTo(in->traversalMemoryContext);
                BOX *quadrant = box_copy(bbox);
                MemoryContextSwitchTo(oldCtx);

                out->traversalValues[i] = quadrant;
                out->distances[i] = spg_key_orderbys_distances(BoxPGetDatum(quadrant), false,
                                                               in->orderbys, in->norderbys);
            }
        }
        PG_RETURN_VOID();
    }

    // Normal case: filter quadrants based on scan keys
    // Start with all 4 quadrants potentially valid
    which = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);

    // Process each scan key to filter quadrants
    for (int i = 0; i < in->nkeys; i++) {
        Point *query = DatumGetPointP(in->scankeys[i].sk_argument);

        switch (in->scankeys[i].sk_strategy) {
            case RTLeftStrategyNumber:
                if (SPTEST(point_right, centroid, query))
                    which &= (1 << 3) | (1 << 4);  // SW, NW quadrants
                break;
            case RTRightStrategyNumber:
                if (SPTEST(point_left, centroid, query))
                    which &= (1 << 1) | (1 << 2);  // NE, SE quadrants
                break;
            case RTSameStrategyNumber:
                which &= (1 << getQuadrant(centroid, query));
                break;
            case RTBelowStrategyNumber:
            case RTOldBelowStrategyNumber:
                if (SPTEST(point_above, centroid, query))
                    which &= (1 << 2) | (1 << 3);  // SE, SW quadrants
                break;
            case RTAboveStrategyNumber:
            case RTOldAboveStrategyNumber:
                if (SPTEST(point_below, centroid, query))
                    which &= (1 << 1) | (1 << 4);  // NE, NW quadrants
                break;
            case RTContainedByStrategyNumber:
                // Query is a box - check quadrants containing all corners
                BOX *boxQuery = DatumGetBoxP(in->scankeys[i].sk_argument);
                if (DatumGetBool(DirectFunctionCall2(box_contain_pt,
                                                     PointerGetDatum(boxQuery),
                                                     PointerGetDatum(centroid)))) {
                    // Centroid in box - all quadrants OK
                } else {
                    // Find quadrants containing all box corners
                    Point p;
                    int r = 0;
                    p = boxQuery->low;
                    r |= 1 << getQuadrant(centroid, &p);
                    p.y = boxQuery->high.y;
                    r |= 1 << getQuadrant(centroid, &p);
                    p = boxQuery->high;
                    r |= 1 << getQuadrant(centroid, &p);
                    p.x = boxQuery->low.x;
                    r |= 1 << getQuadrant(centroid, &p);
                    which &= r;
                }
                break;
            default:
                elog(ERROR, "unrecognized strategy number: %d",
                     in->scankeys[i].sk_strategy);
                break;
        }

        // Early termination if no quadrants remain
        if (which == 0)
            break;
    }

    // Setup output for selected quadrants
    out->levelAdds = palloc(sizeof(int) * 4);
    for (int i = 0; i < 4; ++i)
        out->levelAdds[i] = 1;

    out->nodeNumbers = palloc(sizeof(int) * 4);
    out->nNodes = 0;

    // Add each selected quadrant to results
    for (int i = 1; i <= 4; i++) {
        if (which & (1 << i)) {
            out->nodeNumbers[out->nNodes] = i - 1;

            if (in->norderbys > 0) {
                MemoryContext oldCtx = MemoryContextSwitchTo(in->traversalMemoryContext);
                BOX *quadrant = getQuadrantArea(bbox, centroid, i);
                MemoryContextSwitchTo(oldCtx);

                out->traversalValues[out->nNodes] = quadrant;
                out->distances[out->nNodes] = spg_key_orderbys_distances(BoxPGetDatum(quadrant), false,
                                                                         in->orderbys, in->norderbys);
            }
            out->nNodes++;
        }
    }

    PG_RETURN_VOID();
}
```