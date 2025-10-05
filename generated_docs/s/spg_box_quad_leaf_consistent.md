# spg_box_quad_leaf_consistent

## Location
[src/backend/utils/adt/geo_spgist.c:741-858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L741-L858)

## Overview
A leaf consistency function for SP-GiST quadtree indexes on geometric box and polygon data types that determines whether a leaf node matches the search criteria.

## Definition

```c
Datum
spg_box_quad_leaf_consistent(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
/* SP-GiST leaf consistent function - tests if leaf matches search criteria */
Datum
spg_box_quad_leaf_consistent(PG_FUNCTION_ARGS)
{
    spgLeafConsistentIn *in = (spgLeafConsistentIn *) PG_GETARG_POINTER(0);
    spgLeafConsistentOut *out = (spgLeafConsistentOut *) PG_GETARG_POINTER(1);

    Datum leaf = in->leafDatum;
    bool flag = true;

    // All tests are exact - no recheck needed
    out->recheck = false;

    // Return leaf value if requested
    if (in->returnData)
        out->leafValue = leaf;

    // Test all search constraints
    for (int i = 0; i < in->nkeys; i++) {
        StrategyNumber strategy = in->scankeys[i].sk_strategy;
        BOX *box = spg_box_quad_get_scankey_bbox(&in->scankeys[i], &out->recheck);
        Datum query = BoxPGetDatum(box);

        // Apply spatial relationship test based on strategy
        switch (strategy) {
            case RTOverlapStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_overlap, leaf, query));
                break;
            case RTContainsStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_contain, leaf, query));
                break;
            case RTContainedByStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_contained, leaf, query));
                break;
            case RTSameStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_same, leaf, query));
                break;
            case RTLeftStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_left, leaf, query));
                break;
            case RTOverLeftStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_overleft, leaf, query));
                break;
            case RTRightStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_right, leaf, query));
                break;
            case RTOverRightStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_overright, leaf, query));
                break;
            case RTAboveStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_above, leaf, query));
                break;
            case RTOverAboveStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_overabove, leaf, query));
                break;
            case RTBelowStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_below, leaf, query));
                break;
            case RTOverBelowStrategyNumber:
                flag = DatumGetBool(DirectFunctionCall2(box_overbelow, leaf, query));
                break;
            default:
                elog(ERROR, "unrecognized strategy: %d", strategy);
        }

        // Exit early on first failure
        if (!flag)
            break;
    }

    // Handle distance calculations for ordering queries
    if (flag && in->norderbys > 0) {
        out->distances = spg_key_orderbys_distances(leaf, false,
                                                   in->orderbys, in->norderbys);
        // Polygon distance requires recheck
        out->recheckDistances = (in->orderbys[0].sk_func.fn_oid == F_DIST_POLYP);
    }

    PG_RETURN_BOOL(flag);
}
```