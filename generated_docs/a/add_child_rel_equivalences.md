# add_child_rel_equivalences

## Location
[src/backend/optimizer/path/equivclass.c:2631-2752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L2631-L2752)

## Overview
Creates transformed equivalence class members for a child relation by finding parent relation members and translating them to reference the child relation, supporting PostgreSQL's inheritance and partitioning features.

## Definition
```c
void add_child_rel_equivalences(PlannerInfo *root,
                               AppendRelInfo *appinfo,
                               RelOptInfo *parent_rel,
                               RelOptInfo *child_rel)
```

## Detailed Description
This function handles equivalence class propagation in PostgreSQL's inheritance hierarchy (including table partitioning). When a child relation (partition or inherited table) is being planned, this function ensures that equivalence relationships established for the parent relation are also available for the child relation in appropriately transformed form.

The function works by iterating through all equivalence classes that contain the parent relation, then examining each original (non-child, non-constant) member of those equivalence classes. For members that reference only the top-level parent relations, it creates transformed versions that reference the child relation instead.

The transformation process depends on whether this is a simple single-level inheritance relationship (RELOPT_BASEREL) or a multi-level inheritance hierarchy. For single-level cases, it uses adjust_appendrel_attrs with the provided AppendRelInfo. For multi-level cases, it uses adjust_appendrel_attrs_multilevel to handle the more complex translation.

Key optimizations include: only processing original members (not already-transformed child members) to avoid O(N²) explosion of derived expressions, skipping volatile equivalence classes for safety, and only processing equivalence classes that actually involve the parent relation.

The function updates both the equivalence class membership and the child relation's eclass_indexes to maintain the bidirectional relationship between relations and equivalence classes.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the global query planning context
- `appinfo`: AppendRelInfo describing the parent-child relationship (used for single-level transformations)
- `parent_rel`: RelOptInfo for the parent relation whose equivalence members are being transformed
- `child_rel`: RelOptInfo for the child relation that will receive the transformed equivalence members

## Dependencies
- Functions called/Symbols referenced:
  - [AppendRelInfo](../A/AppendRelInfo.md) (struct type)
  - [EquivalenceClass](../E/EquivalenceClass.md) (struct type)
  - [EquivalenceMember](../E/EquivalenceMember.md) (struct type)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - bms_is_empty
  - [bms_difference](../b/bms_difference.md)
  - [bms_add_members](../b/bms_add_members.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [list_nth](../l/list_nth.md)
  - [adjust_appendrel_attrs](adjust_appendrel_attrs.md)
  - [adjust_appendrel_attrs_multilevel](adjust_appendrel_attrs_multilevel.md)
  - [add_eq_member](add_eq_member.md)
  - IS_SIMPLE_REL (macro)
  - RELOPT_BASEREL (constant)
- Called from (representative examples):
  - [set_append_rel_size](../s/set_append_rel_size.md)
  - Referenced in paths.h header

## Notes and Other Information
- This function is only called when there's reason to believe the generated equivalence members will be useful
- Requires that equivalence class merging has been completed (ec_merging_done assertion)
- Skips volatile equivalence classes entirely for safety reasons
- Only processes original equivalence members, not previously transformed child members
- Excludes parent relation Vars with nonempty varnullingrels to avoid translation failures
- Uses different transformation strategies for single-level vs multi-level inheritance hierarchies
- Updates the child relation's eclass_indexes to maintain the relation-to-equivalence-class mapping
- The transformed expressions maintain the same datatype and join domain as the original members
- Handles both inheritance and partitioning scenarios in PostgreSQL's query planning
- The function is designed to avoid exponential explosion of derived expressions in complex inheritance hierarchies

## Simplified Source

This simplified version focuses on the core equivalence transformation logic:

```c
void add_child_rel_equivalences(PlannerInfo *root,
                               AppendRelInfo *appinfo,
                               RelOptInfo *parent_rel,
                               RelOptInfo *child_rel)
{
    Relids top_parent_relids = child_rel->top_parent_relids;
    Relids child_relids = child_rel->relids;
    int i;

    // Use parent rel's eclass_indexes to iterate efficiently
    Assert(root->ec_merging_done);
    Assert(IS_SIMPLE_REL(parent_rel));

    i = -1;
    while ((i = bms_next_member(parent_rel->eclass_indexes, i)) >= 0)
    {
        EquivalenceClass *cur_ec = (EquivalenceClass *) list_nth(root->eq_classes, i);
        int num_members;

        // Skip volatile equivalence classes for safety
        if (cur_ec->ec_has_volatile)
            continue;

        Assert(bms_is_subset(top_parent_relids, cur_ec->ec_relids));

        // Process only pre-existing members to avoid O(N²) explosion
        num_members = list_length(cur_ec->ec_members);
        for (int pos = 0; pos < num_members; pos++)
        {
            EquivalenceMember *cur_em = (EquivalenceMember *) list_nth(cur_ec->ec_members, pos);

            // Skip constants and already-transformed child members
            if (cur_em->em_is_const || cur_em->em_is_child)
                continue;

            // Only process members that reference top parent rel
            if (bms_is_subset(cur_em->em_relids, top_parent_relids) &&
                !bms_is_empty(cur_em->em_relids))
            {
                Expr *child_expr;
                Relids new_relids;

                // Transform expression for child relation
                if (parent_rel->reloptkind == RELOPT_BASEREL)
                {
                    // Simple single-level transformation
                    child_expr = (Expr *) adjust_appendrel_attrs(root,
                                                               (Node *) cur_em->em_expr,
                                                               1, &appinfo);
                }
                else
                {
                    // Multi-level transformation
                    child_expr = (Expr *) adjust_appendrel_attrs_multilevel(root,
                                                                          (Node *) cur_em->em_expr,
                                                                          child_rel,
                                                                          child_rel->top_parent);
                }

                // Update relids to reference child instead of parent
                new_relids = bms_difference(cur_em->em_relids, top_parent_relids);
                new_relids = bms_add_members(new_relids, child_relids);

                // Add the transformed member to the equivalence class
                (void) add_eq_member(cur_ec, child_expr, new_relids,
                                   cur_em->em_jdomain, cur_em, cur_em->em_datatype);

                // Record this EC index for the child rel
                child_rel->eclass_indexes = bms_add_member(child_rel->eclass_indexes, i);
            }
        }
    }
}
```

**Key simplifications made:**
- Removed extensive explanatory comments while preserving critical logic comments
- Maintained essential safety checks and optimizations
- Preserved the two-tier transformation strategy (single vs multi-level)
- Kept clear variable names and algorithmic structure
- Reduced from ~130 lines to ~60 lines while maintaining all essential functionality