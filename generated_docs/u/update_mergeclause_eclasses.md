# update_mergeclause_eclasses

## Location
src/backend/optimizer/path/pathkeys.c: 1490 - 1523

## Overview
Updates the cached EquivalenceClass links in a mergeclause RestrictInfo to point to canonical merged parent EquivalenceClasses after EC merging is complete.

## Definition


## Detailed Description
The `update_mergeclause_eclasses` function ensures that the EquivalenceClass pointers in a mergeclause RestrictInfo structure point to the correct canonical equivalence classes after the EC merging process is complete. During query planning, EquivalenceClasses may be merged when the planner discovers that expressions are equivalent. When this happens, the original ECs become non-canonical and point to their merged parent via the `ec_merged` field.

This function traverses the `ec_merged` chain for both the left and right EquivalenceClasses of the mergeclause, updating the pointers to reference the final canonical merged parents. This is essential for ensuring that merge join planning and pathkey operations work with the correct, up-to-date equivalence class information.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context (currently unused in the function body)
- `restrictinfo`: RestrictInfo structure for the mergeclause whose EquivalenceClass links need updating

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses direct field access on EquivalenceClass structures)
- Called from (representative examples):
  - [select_mergejoin_clauses](../s/select_mergejoin_clauses.md)
  - [find_mergeclauses_for_outer_pathkeys](../f/find_mergeclauses_for_outer_pathkeys.md)
  - [select_outer_pathkeys_for_merge](../s/select_outer_pathkeys_for_merge.md)
  - [make_inner_pathkeys_for_merge](../m/make_inner_pathkeys_for_merge.md)
  - [pathkeys_useful_for_merging](../p/pathkeys_useful_for_merging.md)

## Notes and Other Information
- Must be called after EC merging is complete to ensure canonical EC references
- Validates that the RestrictInfo is actually a mergeclause (mergeopfamilies != NIL)
- Requires that left_ec and right_ec pointers are already initialized (not NULL)
- Uses simple pointer chasing through the `ec_merged` chain to find canonical parents
- Essential for correct merge join planning and pathkey operations
- The function modifies the RestrictInfo structure in-place to update the EC pointers
- Complements `initialize_mergeclause_eclasses` which sets up the initial EC links