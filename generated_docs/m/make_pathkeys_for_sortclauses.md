# make_pathkeys_for_sortclauses

## Location
[src/backend/optimizer/path/pathkeys.c:1330-1370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L1330-L1370)

## Overview
Generates a pathkeys list that represents the sort order specified by a list of SortGroupClauses, serving as a wrapper for the extended version with default parameters.

## Definition

```c
List *
make_pathkeys_for_sortclauses(PlannerInfo *root,
							  List *sortclauses,
							  List *tlist)
```
## Detailed Description
The `make_pathkeys_for_sortclauses` function is a convenience wrapper around `make_pathkeys_for_sortclauses_extended` that converts a list of SortGroupClause nodes into a canonical pathkeys list. The resulting PathKeys represent the sort order specified by the input clauses and are always in canonical form. The function assumes that all provided sort clauses are sortable and will assert if this condition is not met.

This function is commonly used throughout the PostgreSQL query planner when converting explicit sort specifications (like ORDER BY clauses) into internal pathkey representations that can be used for optimization decisions.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context
- `sortclauses`: List of SortGroupClause nodes specifying the desired sort order
- `tlist`: Target list containing the referenced target list entries for the sort clauses

## Dependencies
- Functions called/Symbols referenced:
  - [make_pathkeys_for_sortclauses_extended](make_pathkeys_for_sortclauses_extended.md)
- Called from (representative examples):
  - [minmax_qp_callback](minmax_qp_callback.md)
  - [grouping_planner](../g/grouping_planner.md)
  - [adjust_group_pathkeys_for_groupagg](../a/adjust_group_pathkeys_for_groupagg.md)
  - [standard_qp_callback](../s/standard_qp_callback.md)
  - [make_pathkeys_for_window](make_pathkeys_for_window.md)
  - [generate_union_paths](../g/generate_union_paths.md)
  - [generate_nonunion_paths](../g/generate_nonunion_paths.md)

## Notes and Other Information
- This is a simplified interface to the more flexible `make_pathkeys_for_sortclauses_extended` function
- All resulting PathKeys are guaranteed to be in canonical form
- The function asserts that all provided sort clauses are sortable - it is a caller error if they are not
- Widely used throughout the planner for converting sort specifications to pathkey representations
- The canonical form ensures that equivalent pathkeys can be efficiently compared and merged