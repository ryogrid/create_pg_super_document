# tlist_same_exprs

## Location
[src/backend/optimizer/util/tlist.c:218-247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L218-L247)

## Overview
Checks whether two target lists contain the same expressions, ignoring labeling attributes that don't affect computed row values.

## Definition

```c
bool
tlist_same_exprs(List *tlist1, List *tlist2)
```
## Detailed Description
This function compares two target lists to determine if they contain the same expressions. It's primarily used to decide whether it's safe to substitute a new target list into a non-projection-capable plan node. The function performs a structural comparison of the expression trees while ignoring TargetEntry attributes like resname, ressortgroupref, resorigtbl, resorigcol, and resjunk, as these are only labelings that don't affect the actual row values computed by the node.

This design choice allows the optimizer to make more aggressive optimizations by recognizing equivalent computations even when they have different labeling metadata. The planner often doesn't bother to maintain valid labeling in intermediate plan nodes, so ignoring these attributes prevents missed optimization opportunities.

## Parameters / Member Variables
- `*tlist1`: First target list to compare
- `*tlist2`: Second target list to compare
## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (implicit via list operations)
  - forboth (macro for iterating over two lists simultaneously)
  - [equal](../e/equal.md) (for comparing expression trees)
  - [TargetEntry](../T/TargetEntry.md) (struct type)
- Called from (representative examples):
  - [create_projection_plan](../c/create_projection_plan.md)
  - [change_plan_targetlist](../c/change_plan_targetlist.md)
  - standard_qp_extra
  - [apply_scanjoin_target_to_paths](../a/apply_scanjoin_target_to_paths.md)

## Notes and Other Information
- Returns false immediately if the two lists have different lengths
- On success, the caller must still substitute the desired target list into the plan node to ensure proper labeling
- The function only compares the expr field of each TargetEntry, not the metadata fields
- This is a critical function for query optimization as it enables plan node reuse and target list substitution

## Simplified Source
```c
bool
tlist_same_exprs(List *tlist1, List *tlist2)
{
    ListCell *lc1, *lc2;

    // Quick length check
    if (list_length(tlist1) != list_length(tlist2))
        return false;

    // Compare each expression pair
    forboth(lc1, tlist1, lc2, tlist2)
    {
        TargetEntry *tle1 = (TargetEntry *) lfirst(lc1);
        TargetEntry *tle2 = (TargetEntry *) lfirst(lc2);

        // Only compare expressions, ignore labeling attributes
        if (!equal(tle1->expr, tle2->expr))
            return false;
    }

    return true;
}
```