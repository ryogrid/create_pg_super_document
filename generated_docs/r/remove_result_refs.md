# remove_result_refs

## Location
[src/backend/optimizer/prep/prepjointree.c:3801-3836](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L3801-L3836)

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
- `*root`: PlannerInfo containing the query tree and global information including PHV tracking
- `varno`: The relation ID of the RTE_RESULT being removed
- `*newjtloc`: The new join tree location where PHVs should be evaluated instead of at the removed RTE
## Dependencies
- Functions called/Symbols referenced:
  - [get_relids_in_jointree](../g/get_relids_in_jointree.md) (to determine available relations at new location)
  - bms_is_empty (to validate that the new location has relations)
  - [substitute_phv_relids](../s/substitute_phv_relids.md) (to update PHV relation references)
  - [fix_append_rel_relids](../f/fix_append_rel_relids.md) (to handle append relation adjustments)

- Called from (representative examples):
  - [remove_useless_results_recurse](remove_useless_results_recurse.md) (5 different locations for various join scenarios)

## Notes and Other Information
- This is a static function, only accessible within prepjointree.c
- Does not physically remove the RTE from the join tree structure - that's handled by the caller
- Assumes the join tree is in a valid state (no disconnected nodes) when called
- Includes an assertion that the new join tree location contains at least one relation
- Optimized to skip processing when no PHVs exist in the query
- The append_rel_list doesn't need processing because RTEs in the main jointree won't be appendrel members
- Part of the RTE_RESULT optimization cleanup infrastructure
- Critical for maintaining query semantic correctness after RTE removal

## Simplified Source

```c
static void remove_result_refs(PlannerInfo *root, int varno, Node *newjtloc)
{
    // Only process PlaceHolderVars if they exist in the query
    if (root->glob->lastPHId != 0)
    {
        Relids subrelids;

        // Get available relations at the new join tree location
        subrelids = get_relids_in_jointree(newjtloc, true, false);
        Assert(!bms_is_empty(subrelids));

        // Update PHVs to reference new location instead of removed RTE
        substitute_phv_relids((Node *) root->parse, varno, subrelids);

        // Handle any append relation adjustments
        fix_append_rel_relids(root, varno, subrelids);
    }

    // Note: PlanRowMark cleanup is deferred to caller
}
```