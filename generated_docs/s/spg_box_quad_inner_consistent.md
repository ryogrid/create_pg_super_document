# spg_box_quad_inner_consistent

## Location
[src/backend/utils/adt/geo_spgist.c:553-740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L553-L740)

## Overview
The SP-GiST inner consistent function for box geometric types that determines which child nodes should be visited during spatial query traversal by evaluating spatial relationships in 4D space.

## Definition
```c
Datum spg_box_quad_inner_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the "inner consistent" operation for SP-GiST indexes on PostgreSQL's BOX geometric type using a quadtree partitioning strategy. During query execution, this function determines which child nodes of an internal node need to be visited based on the spatial relationships between the query constraints and the 4D bounding rectangles representing each quadrant.

The function operates by:
1. Initializing or retrieving the current traversal state (RectBox)
2. Handling the special case where all child nodes are identical (allTheSame)
3. Converting the centroid and query boxes to RangeBox format for 4D operations
4. For each potential child quadrant, calculating the refined bounding rectangle
5. Testing spatial relationships against all query constraints using 4D geometric predicates
6. Collecting qualifying child nodes with their traversal values and distance calculations
7. Managing memory contexts for persistent traversal state

The 4D approach treats each box as having four coordinates (low.x, high.x, low.y, high.y), enabling more precise spatial reasoning than traditional 2D methods.

## Parameters / Member Variables
- `in` (spgInnerConsistentIn*): Input structure containing prefix datum, scan keys, traversal state, and ordering constraints
- `out` (spgInnerConsistentOut*): Output structure where qualifying child nodes, traversal values, and distances are stored
- `rect_box` (RectBox*): Current bounding rectangle representing the traversal state in 4D space
- `centroid` (RangeBox*): The node's centroid converted to 4D range format
- `queries` (RangeBox**): Array of query constraints converted to 4D range format

## Dependencies
- Functions called/Symbols referenced:
  - [initRectBox](../i/initRectBox.md) (initializes unbounded 4D rectangle)
  - [getRangeBox](../g/getRangeBox.md) (converts BOX to RangeBox format)
  - [spg_box_quad_get_scankey_bbox](spg_box_quad_get_scankey_bbox.md) (extracts bounding box from scan key)
  - [nextRectBox](../n/nextRectBox.md) (calculates refined bounding rectangle for quadrant)
  - 4D spatial predicates: overlap4D, contain4D, contained4D, left4D, overLeft4D, right4D, overRight4D, above4D, overAbove4D, below4D, overBelow4D
  - [pointToRectBoxDistance](../p/pointToRectBoxDistance.md) (distance calculation for ordering)
  - [DatumGetBoxP](../D/DatumGetBoxP.md), DatumGetPointP (datum conversion functions)
  - Memory management: palloc, pfree, MemoryContextSwitchTo
- Called from (representative examples):
  - SP-GiST query execution engine
  - Spatial index traversal operations

## Notes and Other Information
- Supports all major spatial relationship strategies: overlap, containment, directional relationships
- Handles ordering queries by calculating point-to-rectangle distances
- Uses persistent memory context for traversal values that survive function calls
- Special handling for allTheSame case where all child nodes have identical data
- Memory optimization by freeing unused traversal values for rejected nodes
- Located in src/backend/utils/adt/geo_spgist.c:553-740
- Critical performance component as it determines index traversal paths during queries

## Simplified Source

```c
/* SP-GiST inner consistent function - determines which child nodes to visit */
Datum
spg_box_quad_inner_consistent(PG_FUNCTION_ARGS)
{
    spgInnerConsistentIn *in = (spgInnerConsistentIn *) PG_GETARG_POINTER(0);
    spgInnerConsistentOut *out = (spgInnerConsistentOut *) PG_GETARG_POINTER(1);

    // Initialize or use existing traversal state
    RectBox *rect_box = in->traversalValue ? in->traversalValue : initRectBox();

    // Special case: if all nodes are identical, visit all
    if (in->allTheSame) {
        out->nNodes = in->nNodes;
        out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
        for (int i = 0; i < in->nNodes; i++)
            out->nodeNumbers[i] = i;

        // Handle ordering if present
        if (in->norderbys > 0 && in->nNodes > 0) {
            // Calculate distances for all nodes (simplified)
            double *distances = palloc(sizeof(double) * in->norderbys);
            for (int j = 0; j < in->norderbys; j++) {
                Point *pt = DatumGetPointP(in->orderbys[j].sk_argument);
                distances[j] = pointToRectBoxDistance(pt, rect_box);
            }
            // Duplicate distances for all nodes
            out->distances = (double **) palloc(sizeof(double *) * in->nNodes);
            for (int i = 0; i < in->nNodes; i++) {
                out->distances[i] = palloc(sizeof(double) * in->norderbys);
                memcpy(out->distances[i], distances, sizeof(double) * in->norderbys);
            }
        }
        PG_RETURN_VOID();
    }

    // Convert centroid and queries to 4D format
    RangeBox *centroid = getRangeBox(DatumGetBoxP(in->prefixDatum));
    RangeBox **queries = (RangeBox **) palloc(in->nkeys * sizeof(RangeBox *));
    for (int i = 0; i < in->nkeys; i++) {
        BOX *box = spg_box_quad_get_scankey_bbox(&in->scankeys[i], NULL);
        queries[i] = getRangeBox(box);
    }

    // Prepare output arrays
    out->nNodes = 0;
    out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
    out->traversalValues = (void **) palloc(sizeof(void *) * in->nNodes);
    if (in->norderbys > 0)
        out->distances = (double **) palloc(sizeof(double *) * in->nNodes);

    // Switch to persistent memory context
    MemoryContext old_ctx = MemoryContextSwitchTo(in->traversalMemoryContext);

    // Test each quadrant
    for (uint8 quadrant = 0; quadrant < in->nNodes; quadrant++) {
        RectBox *next_rect_box = nextRectBox(rect_box, centroid, quadrant);
        bool flag = true;

        // Test all query constraints
        for (int i = 0; i < in->nkeys && flag; i++) {
            StrategyNumber strategy = in->scankeys[i].sk_strategy;

            switch (strategy) {
                case RTOverlapStrategyNumber:
                    flag = overlap4D(next_rect_box, queries[i]); break;
                case RTContainsStrategyNumber:
                    flag = contain4D(next_rect_box, queries[i]); break;
                case RTSameStrategyNumber:
                case RTContainedByStrategyNumber:
                    flag = contained4D(next_rect_box, queries[i]); break;
                case RTLeftStrategyNumber:
                    flag = left4D(next_rect_box, queries[i]); break;
                case RTOverLeftStrategyNumber:
                    flag = overLeft4D(next_rect_box, queries[i]); break;
                case RTRightStrategyNumber:
                    flag = right4D(next_rect_box, queries[i]); break;
                case RTOverRightStrategyNumber:
                    flag = overRight4D(next_rect_box, queries[i]); break;
                case RTAboveStrategyNumber:
                    flag = above4D(next_rect_box, queries[i]); break;
                case RTOverAboveStrategyNumber:
                    flag = overAbove4D(next_rect_box, queries[i]); break;
                case RTBelowStrategyNumber:
                    flag = below4D(next_rect_box, queries[i]); break;
                case RTOverBelowStrategyNumber:
                    flag = overBelow4D(next_rect_box, queries[i]); break;
                default:
                    elog(ERROR, "unrecognized strategy: %d", strategy);
            }
        }

        // Include qualifying nodes
        if (flag) {
            out->traversalValues[out->nNodes] = next_rect_box;
            out->nodeNumbers[out->nNodes] = quadrant;

            // Calculate distances for ordering if needed
            if (in->norderbys > 0) {
                double *distances = palloc(sizeof(double) * in->norderbys);
                out->distances[out->nNodes] = distances;
                for (int j = 0; j < in->norderbys; j++) {
                    Point *pt = DatumGetPointP(in->orderbys[j].sk_argument);
                    distances[j] = pointToRectBoxDistance(pt, next_rect_box);
                }
            }
            out->nNodes++;
        } else {
            pfree(next_rect_box);  // Free unused traversal value
        }
    }

    MemoryContextSwitchTo(old_ctx);
    PG_RETURN_VOID();
}
```