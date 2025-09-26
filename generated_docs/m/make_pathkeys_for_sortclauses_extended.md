# make_pathkeys_for_sortclauses_extended

## Location
[src/backend/optimizer/path/pathkeys.c:1371-1442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L1371-L1442)

## Overview
The extended version of pathkey generation that converts SortGroupClauses to pathkeys with additional options for redundancy removal, sortability checking, and equivalence class reference setting.

## Definition

```c
structed from a WHERE clause, i.e.
			 * it doesn't have a target reference at all.
			 */
			pathkey->pk_eclass->ec_sortref = sortcl->tleSortGroupRef;
```
## Detailed Description
The `make_pathkeys_for_sortclauses_extended` function provides comprehensive pathkey generation from SortGroupClause lists with advanced control options. It processes each sort clause to create corresponding PathKey objects, handling cases where sort operators are invalid (unsortable clauses). The function can optionally remove redundant sort clauses from the input list and set equivalence class sort references. 

The function maintains canonical form by eliminating redundant ordering keys and can report whether all clauses were successfully converted to sortable pathkeys. Even when some clauses are unsortable, the function continues processing to identify and potentially remove redundant clauses, optimizing the final sort operation.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context
- `sortclauses`: Pointer to list of SortGroupClause nodes (pass-by-reference to allow modification)
- `tlist`: Target list containing referenced target list entries for the sort clauses
- `remove_redundant`: If true, removes sort clauses that give rise to redundant pathkeys from the sortclauses list
- `sortable`: Output parameter set to true if all sort clauses are sortable, false otherwise
- `set_ec_sortref`: If true, sets the pathkey's EquivalenceClass sortref value when not already initialized

## Dependencies
- Functions called/Symbols referenced:
  - [SortGroupClause](../S/SortGroupClause.md) (struct type)
  - [PathKey](../P/PathKey.md) (struct type)
  - [get_sortgroupclause_expr](../g/get_sortgroupclause_expr.md)
  - [make_pathkey_from_sortop](make_pathkey_from_sortop.md)
  - [pathkey_is_redundant](../p/pathkey_is_redundant.md)
  - foreach_delete_current
- Called from (representative examples):
  - [make_pathkeys_for_sortclauses](make_pathkeys_for_sortclauses.md)
  - [standard_qp_callback](../s/standard_qp_callback.md)
  - [make_pathkeys_for_window](make_pathkeys_for_window.md)

## Notes and Other Information
- This is the comprehensive version of pathkey generation with maximum flexibility and control
- Continues processing even when encountering unsortable clauses to identify redundant ones
- The canonical form ensures efficient pathkey comparison and eliminates unnecessary sort operations
- The `remove_redundant` feature helps optimize queries by eliminating unnecessary sort columns
- Setting equivalence class sort references is important for window function processing and grouping operations
- Invalid sort operators (OidIsValid check fails) cause clauses to be marked as unsortable but processing continues