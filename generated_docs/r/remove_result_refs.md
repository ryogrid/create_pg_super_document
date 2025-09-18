# remove_result_refs

## Location
src/backend/optimizer/prep/prepjointree.c: 3801 - 3836

## Overview
A helper routine that performs necessary cleanup when dropping an RTE_RESULT RTE, specifically adjusting PlaceHolderVars that reference the removed RTE to be evaluated at a new location.

## Definition
```c
static void remove_result_refs(PlannerInfo *root, int varno, Node *newjtloc)
```

## Detailed Description
This function handles the cleanup required when an RTE_RESULT is being logically removed from the join tree. While the physical removal of the RTE from the join tree is handled elsewhere, this function ensures that any PlaceHolderVars (PHVs) that referenced the removed RTE are properly updated.

The key operation is reassigning PHVs that were to be evaluated at the removed RTE_RESULT to instead be evaluated at a new join tree location. This involves:

1. **PHV relocation**: Gets the set of relation IDs available at the new join tree location
2. **PHV substitution**: Updates all PHVs in the parse tree that referenced the removed RTE
3. **Append relation handling**: Updates any append relation references

The function includes an optimization to skip PHV processing entirely if no PHVs exist in the query (lastPHId == 0).

Note that PlanRowMark cleanup is deferred to the caller (remove_useless_result_rtes) to avoid redundant work.

## Parameters / Member Variables
- : PlannerInfo containing the query tree and global information including PHV tracking
- : The relation ID of the RTE_RESULT being removed
- : The new join tree location where PHVs should be evaluated instead of at the removed RTE

## Dependencies
- Functions called/Symbols referenced:
  - get_relids_in_jointree (to determine available relations at new location)
  - bms_is_empty (to validate that the new location has relations)
  - substitute_phv_relids (to update PHV relation references)
  - fix_append_rel_relids (to handle append relation adjustments)

- Called from (representative examples):
  - remove_useless_results_recurse (5 different locations for various join scenarios)

## Notes and Other Information
- This is a static function, only accessible within prepjointree.c
- Does not physically remove the RTE from the join tree structure - that's handled by the caller
- Assumes the join tree is in a valid state (no disconnected nodes) when called
- Includes an assertion that the new join tree location contains at least one relation
- Optimized to skip processing when no PHVs exist in the query
- The append_rel_list doesn't need processing because RTEs in the main jointree won't be appendrel members
- Part of the RTE_RESULT optimization cleanup infrastructure
- Critical for maintaining query semantic correctness after RTE removal