# spg_kd_inner_consistent

## Location
[src/backend/access/spgist/spgkdtreeproc.c:160-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgkdtreeproc.c#L160-L349)

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
  - [DatumGetFloat8](../D/DatumGetFloat8.md)
  - [DatumGetPointP](../D/DatumGetPointP.md)
  - [DatumGetBoxP](../D/DatumGetBoxP.md)
  - [FPlt](../F/FPlt.md), FPgt (floating point comparisons)
  - [get_float8_infinity](../g/get_float8_infinity.md)
  - [box_copy](../b/box_copy.md)
  - [spg_key_orderbys_distances](spg_key_orderbys_distances.md)
  - [BoxPGetDatum](../B/BoxPGetDatum.md)
  - [palloc](../p/palloc.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
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

## Simplified Source

```c
Datum spg_kd_inner_consistent(PG_FUNCTION_ARGS) {
    spgInnerConsistentIn *in = (spgInnerConsistentIn *) PG_GETARG_POINTER(0);
    spgInnerConsistentOut *out = (spgInnerConsistentOut *) PG_GETARG_POINTER(1);
    double coord;
    int which, i;
    BOX bboxes[2];

    // Extract splitting coordinate
    Assert(in->hasPrefix);
    coord = DatumGetFloat8(in->prefixDatum);

    // K-d trees should never have allTheSame
    if (in->allTheSame)
        elog(ERROR, "allTheSame should not occur for k-d trees");

    Assert(in->nNodes == 2);

    // Start with both children candidates (bitmask: bit 1=left, bit 2=right)
    which = (1 << 1) | (1 << 2);

    // Process each query constraint
    for (i = 0; i < in->nkeys; i++) {
        Point *query = DatumGetPointP(in->scankeys[i].sk_argument);
        BOX *boxQuery;

        switch (in->scankeys[i].sk_strategy) {
            case RTLeftStrategyNumber:
                // Point must be left of coordinate (X dimension only)
                if ((in->level % 2) != 0 && FPlt(query->x, coord))
                    which &= (1 << 1);
                break;

            case RTRightStrategyNumber:
                // Point must be right of coordinate (X dimension only)
                if ((in->level % 2) != 0 && FPgt(query->x, coord))
                    which &= (1 << 2);
                break;

            case RTSameStrategyNumber:
                // Exact point match - choose appropriate child
                if ((in->level % 2) != 0) {  // X dimension
                    if (FPlt(query->x, coord))
                        which &= (1 << 1);
                    else if (FPgt(query->x, coord))
                        which &= (1 << 2);
                } else {  // Y dimension
                    if (FPlt(query->y, coord))
                        which &= (1 << 1);
                    else if (FPgt(query->y, coord))
                        which &= (1 << 2);
                }
                break;

            case RTBelowStrategyNumber:
            case RTOldBelowStrategyNumber:
                // Point must be below coordinate (Y dimension only)
                if ((in->level % 2) == 0 && FPlt(query->y, coord))
                    which &= (1 << 1);
                break;

            case RTAboveStrategyNumber:
            case RTOldAboveStrategyNumber:
                // Point must be above coordinate (Y dimension only)
                if ((in->level % 2) == 0 && FPgt(query->y, coord))
                    which &= (1 << 2);
                break;

            case RTContainedByStrategyNumber:
                // Query is a box - check containment
                boxQuery = DatumGetBoxP(in->scankeys[i].sk_argument);
                if ((in->level % 2) != 0) {  // X dimension
                    if (FPlt(boxQuery->high.x, coord))
                        which &= (1 << 1);
                    else if (FPgt(boxQuery->low.x, coord))
                        which &= (1 << 2);
                } else {  // Y dimension
                    if (FPlt(boxQuery->high.y, coord))
                        which &= (1 << 1);
                    else if (FPgt(boxQuery->low.y, coord))
                        which &= (1 << 2);
                }
                break;

            default:
                elog(ERROR, "unrecognized strategy number: %d", in->scankeys[i].sk_strategy);
        }

        // Early exit if no children match
        if (which == 0)
            break;
    }

    // Setup output for matching children
    out->nNodes = 0;
    if (!which)
        PG_RETURN_VOID();

    out->nodeNumbers = (int *) palloc(sizeof(int) * 2);

    // Handle distance calculations for nearest neighbor queries
    if (in->norderbys > 0) {
        BOX infArea, *area;

        out->distances = (double **) palloc(sizeof(double *) * in->nNodes);
        out->traversalValues = (void **) palloc(sizeof(void *) * in->nNodes);

        // Setup bounding box for current level
        if (in->level == 0) {
            float8 inf = get_float8_infinity();
            infArea.high.x = infArea.high.y = inf;
            infArea.low.x = infArea.low.y = -inf;
            area = &infArea;
        } else {
            area = (BOX *) in->traversalValue;
        }

        // Split parent bounding box by current coordinate
        bboxes[0].low = area->low;
        bboxes[1].high = area->high;

        if (in->level % 2) {  // Split by X
            bboxes[0].high.x = bboxes[1].low.x = coord;
            bboxes[0].high.y = area->high.y;
            bboxes[1].low.y = area->low.y;
        } else {  // Split by Y
            bboxes[0].high.y = bboxes[1].low.y = coord;
            bboxes[0].high.x = area->high.x;
            bboxes[1].low.x = area->low.x;
        }
    }

    // Build output for each matching child
    for (i = 1; i <= 2; i++) {
        if (which & (1 << i)) {
            out->nodeNumbers[out->nNodes] = i - 1;

            if (in->norderbys > 0) {
                MemoryContext oldCtx = MemoryContextSwitchTo(in->traversalMemoryContext);
                BOX *box = box_copy(&bboxes[i - 1]);
                MemoryContextSwitchTo(oldCtx);

                out->traversalValues[out->nNodes] = box;
                out->distances[out->nNodes] = spg_key_orderbys_distances(BoxPGetDatum(box), false,
                                                                        in->orderbys, in->norderbys);
            }
            out->nNodes++;
        }
    }

    // Set level increments
    out->levelAdds = (int *) palloc(sizeof(int) * 2);
    out->levelAdds[0] = out->levelAdds[1] = 1;

    PG_RETURN_VOID();
}
```