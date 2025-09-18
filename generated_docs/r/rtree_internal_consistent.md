# rtree_internal_consistent

## Location
src/backend/access/gist/gistproc.c: 957 - 1034

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
  - box_overright, box_right, box_overlap, box_left, box_overleft
  - box_contain, box_above, box_overabove, box_overbelow, box_below
  - Strategy number constants (RTLeftStrategyNumber, RTOverlapStrategyNumber, etc.)
- Called from (representative examples):
  - gist_box_consistent
  - gist_poly_consistent  
  - gist_circle_consistent

## Notes and Other Information
- This is a static helper function shared by multiple R-tree data types (box, polygon, circle)
- Uses modified spatial logic appropriate for internal pages where keys represent bounding boxes
- Many strategies use negated conditions to allow for potential matches within bounding boxes
- The function consolidates RTSameStrategyNumber and RTContainsStrategyNumber cases
- Part of the common R-tree functionality that enables spatial indexing for multiple geometric types