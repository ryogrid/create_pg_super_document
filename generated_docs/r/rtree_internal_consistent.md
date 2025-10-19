# rtree_internal_consistent

## Location
[src/backend/access/gist/gistproc.c:957-1034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L957-L1034)

## Overview
Performs internal-page consistency checking for R-tree data types (boxes, polygons, and circles) in GiST indexes by determining if a bounding box can potentially contain query results using appropriate spatial logic.

## Definition
```c
static bool rtree_internal_consistent(BOX *key, BOX *query, StrategyNumber strategy)
```

## Detailed Description
This function implements internal-page consistency checking for all R-tree compatible data types in PostgreSQL's GiST index access method. At internal nodes of a GiST index, this function determines whether a subtree (represented by a bounding box key) could potentially contain tuples that satisfy the query condition. 

Unlike leaf-level consistency checking, internal-page consistency uses modified logic that accounts for the fact that the key represents a bounding box that may contain multiple actual geometries. The function often uses negated conditions - for example, for a 'left-of' query, it checks that the bounding box is NOT 'over-right' of the query box, allowing for the possibility that some contained geometry could still be left of the query.

The function handles cases where RTSameStrategyNumber and RTContainsStrategyNumber use the same logic (both check containment), and RTContainedByStrategyNumber uses overlap logic since any overlapping bounding box might contain a geometry that is contained by the query.

## Parameters / Member Variables
- `key`: Pointer to the BOX representing the bounding box of a subtree at an internal GiST node
- `query`: Pointer to the BOX representing the query condition
- `strategy`: Strategy number indicating which spatial relationship to test for potential matches

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall2 (for calling box operators)  
  - [box_overright](../b/box_overright.md), box_right, box_overlap, box_left, box_overleft
  - [box_contain](../b/box_contain.md), box_above, box_overabove, box_overbelow, box_below
  - Strategy number constants (RTLeftStrategyNumber, RTOverlapStrategyNumber, etc.)
- Called from (representative examples):
  - [gist_box_consistent](../g/gist_box_consistent.md)
  - [gist_poly_consistent](../g/gist_poly_consistent.md)  
  - [gist_circle_consistent](../g/gist_circle_consistent.md)

## Notes and Other Information
- This is a static helper function shared by multiple R-tree data types (box, polygon, circle)
- Uses modified spatial logic appropriate for internal pages where keys represent bounding boxes
- Many strategies use negated conditions to allow for potential matches within bounding boxes
- The function consolidates RTSameStrategyNumber and RTContainsStrategyNumber cases
- Part of the common R-tree functionality that enables spatial indexing for multiple geometric types

## Simplified Source

```c
static bool rtree_internal_consistent(BOX *key, BOX *query, StrategyNumber strategy)
{
    bool retval;

    // Internal node logic: check if subtree might contain qualifying results
    switch (strategy)
    {
        case RTLeftStrategyNumber:
            // Allow if key is not completely to the over-right of query
            retval = !DatumGetBool(DirectFunctionCall2(box_overright,
                                                     PointerGetDatum(key),
                                                     PointerGetDatum(query)));
            break;
        case RTOverLeftStrategyNumber:
            // Allow if key is not completely to the right of query
            retval = !DatumGetBool(DirectFunctionCall2(box_right,
                                                     PointerGetDatum(key),
                                                     PointerGetDatum(query)));
            break;
        case RTOverlapStrategyNumber:
            // Must actually overlap
            retval = DatumGetBool(DirectFunctionCall2(box_overlap,
                                                    PointerGetDatum(key),
                                                    PointerGetDatum(query)));
            break;
        case RTOverRightStrategyNumber:
            // Allow if key is not completely to the left of query
            retval = !DatumGetBool(DirectFunctionCall2(box_left,
                                                     PointerGetDatum(key),
                                                     PointerGetDatum(query)));
            break;
        case RTRightStrategyNumber:
            // Allow if key is not completely to the over-left of query
            retval = !DatumGetBool(DirectFunctionCall2(box_overleft,
                                                     PointerGetDatum(key),
                                                     PointerGetDatum(query)));
            break;
        case RTSameStrategyNumber:
        case RTContainsStrategyNumber:
            // For both same and contains: key must contain query
            retval = DatumGetBool(DirectFunctionCall2(box_contain,
                                                    PointerGetDatum(key),
                                                    PointerGetDatum(query)));
            break;
        case RTContainedByStrategyNumber:
            // Any overlap might contain a contained geometry
            retval = DatumGetBool(DirectFunctionCall2(box_overlap,
                                                    PointerGetDatum(key),
                                                    PointerGetDatum(query)));
            break;
        case RTOverBelowStrategyNumber:
            // Allow if key is not completely above query
            retval = !DatumGetBool(DirectFunctionCall2(box_above,
                                                     PointerGetDatum(key),
                                                     PointerGetDatum(query)));
            break;
        case RTBelowStrategyNumber:
            // Allow if key is not completely over-above query
            retval = !DatumGetBool(DirectFunctionCall2(box_overabove,
                                                     PointerGetDatum(key),
                                                     PointerGetDatum(query)));
            break;
        case RTAboveStrategyNumber:
            // Allow if key is not completely over-below query
            retval = !DatumGetBool(DirectFunctionCall2(box_overbelow,
                                                     PointerGetDatum(key),
                                                     PointerGetDatum(query)));
            break;
        case RTOverAboveStrategyNumber:
            // Allow if key is not completely below query
            retval = !DatumGetBool(DirectFunctionCall2(box_below,
                                                     PointerGetDatum(key),
                                                     PointerGetDatum(query)));
            break;
        default:
            elog(ERROR, "unrecognized strategy number: %d", strategy);
            retval = false;  // Keep compiler quiet
            break;
    }

    return retval;
}
```