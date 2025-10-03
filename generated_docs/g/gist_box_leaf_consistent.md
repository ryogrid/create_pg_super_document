# gist_box_leaf_consistent

## Location
[src/backend/access/gist/gistproc.c:872-956](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L872-L956)

## Overview
Performs leaf-level consistency checking for box data types in GiST (Generalized Search Tree) indexes by applying the appropriate spatial query operator based on the strategy number.

## Definition

```c
static bool
gist_box_leaf_consistent(BOX *key, BOX *query, StrategyNumber strategy)
```
## Detailed Description
This function implements leaf-level consistency checking for BOX data types in PostgreSQL's GiST index access method. At leaf nodes of a GiST index, this function determines whether a stored box (key) satisfies the query condition represented by another box (query) and a strategy number. The function acts as a dispatcher that selects the appropriate box comparison operator based on the strategy number, which corresponds to different spatial relationships like containment, overlap, positioning, etc.

The function supports all standard R-tree spatial strategies by mapping each strategy number to its corresponding box operator function and calling it via PostgreSQL's DirectFunctionCall2 interface.

## Parameters / Member Variables
- `*key`: Pointer to the BOX stored in the leaf node of the GiST index
- `*query`: Pointer to the BOX representing the query condition
- `strategy`: Strategy number indicating which spatial operator to apply (e.g., overlap, contains, left-of, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall2 (for calling box operators)
  - [box_left](../b/box_left.md), box_overleft, box_overlap, box_overright, box_right
  - [box_same](../b/box_same.md), box_contain, box_contained
  - [box_overbelow](../b/box_overbelow.md), box_below, box_above, box_overabove
  - Strategy number constants (RTLeftStrategyNumber, RTOverlapStrategyNumber, etc.)
- Called from (representative examples):
  - [gist_box_consistent](gist_box_consistent.md)

## Notes and Other Information
- This is a static helper function used specifically for leaf-level consistency checking
- The function handles 12 different spatial relationship strategies
- Uses PostgreSQL's DirectFunctionCall2 mechanism to invoke the appropriate box comparison function
- Returns false and logs an error for unrecognized strategy numbers
- Part of the GiST framework's consistent method implementation for box data types