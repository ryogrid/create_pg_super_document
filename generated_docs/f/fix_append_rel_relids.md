# fix_append_rel_relids

## Location
[src/backend/optimizer/prep/prepjointree.c:4037-4080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L4037-L4080)

## Overview
Updates RT-index fields in AppendRelInfo nodes and their translated variables when a subquery is pulled up and relation IDs need to be remapped.

## Definition

```c
static void
fix_append_rel_relids(PlannerInfo *root, int varno, Relids subrelids)
```
## Detailed Description
This function handles the updating of AppendRelInfo nodes when a subquery pullup operation requires remapping relation identifiers. It performs two main tasks:

1. **AppendRelInfo RT-index updating**: Searches through the append_rel_list to find any AppendRelInfo nodes whose child_relid matches the old varno, and updates them to use the new relation ID from subrelids.

2. **PlaceHolderVar updating**: Applies substitute_phv_relids to the translated_vars lists of AppendRelInfo nodes, since these lists might contain PlaceHolderVars that also need their relation ID references updated.

The function includes an optimization to extract the singleton member from subrelids only when needed (lazy evaluation), and includes an assertion that parent_relid should never be a pullup target, which helps catch logic errors.

The function modifies AppendRelInfo nodes in-place, which is safe in this context since they're part of the planner's working data structures.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing the query planning state and append relation list
- `varno`: The old relation ID that needs to be replaced in AppendRelInfo child_relid fields
- `subrelids`: The set of relation IDs to substitute (expected to be singleton in this context)
## Dependencies
- Functions called/Symbols referenced:
  - [AppendRelInfo](../A/AppendRelInfo.md) (structure representing append relation information)
  - [bms_singleton_member](../b/bms_singleton_member.md) (extracts single member from singleton bitmapset)
  - [substitute_phv_relids](../s/substitute_phv_relids.md) (updates PlaceHolderVar relation IDs)
- Called from (representative examples):
  - [pull_up_simple_subquery](../p/pull_up_simple_subquery.md) (in prepjointree.c:1400)
  - [remove_result_refs](../r/remove_result_refs.md) (in prepjointree.c:3812)

## Notes and Other Information
- This function is static and only used within prepjointree.c
- Part of the subquery pullup process in PostgreSQL query optimization
- Modifies AppendRelInfo nodes in-place for performance
- Includes lazy evaluation of subrelids singleton member extraction
- Contains assertion to verify parent_relid is never a pullup target (safety check)
- Only processes PlaceHolderVars if they exist in the query (lastPHId optimization)
- The function expects subrelids to be a singleton set but delays validation until actually needed
- Critical for maintaining correct relation references after inheritance or partitioning expansion during subquery pullup

## Simplified Source

```c
static void
fix_append_rel_relids(PlannerInfo *root, int varno, Relids subrelids)
{
    ListCell *l;
    int subvarno = -1;

    // Process each AppendRelInfo in the list
    foreach(l, root->append_rel_list)
    {
        AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);

        // Parent should never be a pullup target
        Assert(appinfo->parent_relid != varno);

        // Update child_relid if it matches varno
        if (appinfo->child_relid == varno)
        {
            // Extract singleton member only when needed
            if (subvarno < 0)
                subvarno = bms_singleton_member(subrelids);
            appinfo->child_relid = subvarno;
        }

        // Fix PlaceHolderVars in translated_vars if any exist
        if (root->glob->lastPHId != 0)
            substitute_phv_relids((Node *) appinfo->translated_vars,
                                varno, subrelids);
    }
}
```