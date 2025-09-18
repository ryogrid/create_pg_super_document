# apply_pathtarget_labeling_to_tlist

## Location
[src/backend/optimizer/util/tlist.c:774-880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L774-L880)

## Overview
Applies any sortgrouprefs from a PathTarget to matching entries in a target list, enabling proper labeling for ORDER BY and GROUP BY operations.

## Definition
void apply_pathtarget_labeling_to_tlist(List *tlist, PathTarget *target)

## Detailed Description
This function transfers sortgroupref labels from a PathTarget to matching entries in a target list (tlist). Unlike other PathTarget functions, this does not assume a one-for-one correspondence between tlist entries and PathTarget expressions. It's designed to handle cases where createplan.c has decided to use a different target list and matching entries need to be identified.

The function iterates through the PathTarget's expressions and, for each one with a non-zero sortgroupref, finds the corresponding entry in the target list. It uses different matching strategies depending on the expression type: for Var nodes, it uses tlist_member_match_var's weakened matching rules to handle cases where set-returning functions have been inlined, providing more knowledge about return values than when the original Var was created. For other expression types, it uses regular equal() matching.

The function includes error checking to ensure that sortgroupref labels are applied correctly - it will error if no matching target list entry is found, or if attempting to label a column that already has a different sortgroupref label.

## Parameters / Member Variables
- tlist: The target list (List of TargetEntry nodes) to which sortgroupref labels should be applied
- target: The PathTarget containing the sortgrouprefs data to be transferred

## Dependencies
- Functions called/Symbols referenced:
  - [tlist_member_match_var](../t/tlist_member_match_var.md) (used for weakened matching of Var expressions)
  - [tlist_member](../t/tlist_member.md) (used for exact matching of non-Var expressions)
  - [PathTarget](../P/PathTarget.md) (source of sortgrouprefs data)
- Called from (representative examples):
  - [create_scan_plan](../c/create_scan_plan.md) (in src/backend/optimizer/plan/createplan.c:655, 669)
  - [create_projection_plan](../c/create_projection_plan.md) (in src/backend/optimizer/plan/createplan.c:2049)

## Notes and Other Information
- Does not assume one-for-one correspondence between tlist entries and PathTarget expressions
- Uses different matching strategies for Var vs non-Var expressions to handle inlined set-returning functions
- Includes comprehensive error checking for missing expressions and conflicting sortgroupref labels
- Primarily used during plan creation when target lists need to be reconciled with PathTarget labeling
- The weakened matching for Vars handles cases where set-returning function inlining has provided additional type information