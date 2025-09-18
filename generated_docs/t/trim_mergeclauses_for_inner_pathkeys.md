# trim_mergeclauses_for_inner_pathkeys

## Location
src/backend/optimizer/path/pathkeys.c: 1938 - 2032

## Overview
This function trims a list of mergeclauses to include only those that work with a specified ordering for the join's inner relation.

## Definition


## Detailed Description
The function addresses a specific problem in merge join planning: when the inner relation has a pathkey ordering that is a truncation of the ideal pathkeys from make_inner_pathkeys_for_merge, some mergeclauses may need to be dropped even if they match surviving pathkeys. This happens because make_inner_pathkeys_for_merge can reorder pathkeys due to redundancy elimination.

The algorithm works by:
1. **Sequential Pathkey Processing**: Iterates through pathkeys in order, tracking the current pathkey's equivalence class
2. **Mergeclause Matching**: For each mergeclause, checks if its inner-side equivalence class matches the current pathkey
3. **Pathkey Advancement**: When no match is found, advances to the next pathkey only if the current pathkey had at least one matching clause
4. **Early Termination**: Stops when encountering a mergeclause that cannot match any remaining pathkey

The function ensures that the returned mergeclauses maintain the required sequential relationship with the provided pathkeys.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : List of RestrictInfos for mergejoin clauses in an order that works with the outer relation's sort ordering, marked with outer_is_left indicators
- : Pathkeys list showing the ordering of an inner-rel path, typically a truncation of make_inner_pathkeys_for_merge's result

## Dependencies
- Functions called/Symbols referenced:
  - list_head
  - [lnext](../l/lnext.md)  
  - PathKey
  - EquivalenceClass
- Called from (representative examples):
  - [generate_mergejoin_paths](../g/generate_mergejoin_paths.md) (src/backend/optimizer/path/joinpath.c:1619)
  - [generate_mergejoin_paths](../g/generate_mergejoin_paths.md) (src/backend/optimizer/path/joinpath.c:1662)

## Notes and Other Information
- Returns a prefix of the given mergeclauses list, never reordering
- Returns NIL if no pathkeys are provided (though this case is not expected)
- Assumes update_mergeclause_eclasses has already been called on the mergeclauses
- Required because make_inner_pathkeys_for_merge's output ordering may not match mergeclauses ordering
- Handles cases where pathkey truncation requires dropping mergeclauses that would otherwise be usable
- Essential for maintaining correctness when using pre-sorted inner relations in merge joins