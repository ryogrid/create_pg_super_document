# add_child_join_rel_equivalences

## Location
[src/backend/optimizer/path/equivclass.c:2753-2882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L2753-L2882)

## Overview
Creates equivalence class members for child joinrels by transforming expressions from the parent joinrel's equivalence classes.

## Definition

```c
structure after the GEQO context is reset.  This is
	 * problematic since we'll leak memory across repeated GEQO cycles.  For
	 * now, though, bloat is better than crash.  If it becomes a real issue
	 * we'll have to do something to avoid generating duplicate EC members.
	 */
	oldcontext = MemoryContextSwitchTo(root->planner_cxt);
```
## Detailed Description
This function is responsible for propagating equivalence class information from parent joinrels to their child joinrels in partitioned table scenarios. It finds equivalence classes relevant to the top parent joinrel and generates transformed member expressions that reference the child joinrel instead. The function performs expression transformation using appendrel attribute adjustment, ensuring that equivalence relationships are maintained across the partition hierarchy.

The function handles both simple single-level transformations (RELOPT_JOINREL) and complex multi-level transformations (RELOPT_OTHER_JOINREL). It carefully manages memory context to avoid corruption during GEQO planning and skips volatile expressions for safety.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning state and equivalence class information
- : Number of AppendRelInfo structures in the appinfos array
- : Array of AppendRelInfo structures describing the parent-child relationships
- : The parent joinrel whose equivalence classes are being propagated
- : The child joinrel that will receive the transformed equivalence class members

## Dependencies
- Functions called/Symbols referenced:
  - [get_eclass_indexes_for_relids](../g/get_eclass_indexes_for_relids.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [list_nth](../l/list_nth.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_membership](../b/bms_membership.md)
  - [adjust_appendrel_attrs](adjust_appendrel_attrs.md)
  - [adjust_appendrel_attrs_multilevel](adjust_appendrel_attrs_multilevel.md)
  - [bms_difference](../b/bms_difference.md)
  - [bms_add_members](../b/bms_add_members.md)
  - [add_eq_member](add_eq_member.md)
- Called from (representative examples):
  - build_child_join_rel

## Notes and Other Information
- Only processes equivalence classes that contain multi-relational expressions (skips single baserel expressions)
- Skips volatile equivalence classes to avoid dangerous transformations
- Uses main planner context during GEQO planning to prevent memory corruption
- Handles both simple and multi-level partition hierarchies
- Part of PostgreSQL's query optimization framework for partitioned tables
- Located in src/backend/optimizer/path/equivclass.c:2753-2882

## Simplified Source

```c
void
add_child_join_rel_equivalences(PlannerInfo *root,
                               int nappinfos, AppendRelInfo **appinfos,
                               RelOptInfo *parent_joinrel,
                               RelOptInfo *child_joinrel)
{
    Relids top_parent_relids = child_joinrel->top_parent_relids;
    Relids child_relids = child_joinrel->relids;

    // Find equivalence classes that reference the parent joinrel
    Bitmapset *matching_ecs = get_eclass_indexes_for_relids(root, top_parent_relids);

    // Switch to main planner context to avoid GEQO memory corruption
    MemoryContext oldcontext = MemoryContextSwitchTo(root->planner_cxt);

    // Process each matching equivalence class
    int i = -1;
    while ((i = bms_next_member(matching_ecs, i)) >= 0) {
        EquivalenceClass *cur_ec = (EquivalenceClass *) list_nth(root->eq_classes, i);

        // Skip volatile equivalence classes for safety
        if (cur_ec->ec_has_volatile)
            continue;

        // Process existing members (not newly added child members)
        int num_members = list_length(cur_ec->ec_members);
        for (int pos = 0; pos < num_members; pos++) {
            EquivalenceMember *cur_em = (EquivalenceMember *) list_nth(cur_ec->ec_members, pos);

            // Skip constants and already-transformed child members
            if (cur_em->em_is_const || cur_em->em_is_child)
                continue;

            // Skip single-baserel expressions (handled elsewhere)
            if (bms_membership(cur_em->em_relids) != BMS_MULTIPLE)
                continue;

            // Check if member references the parent rel
            if (bms_overlap(cur_em->em_relids, top_parent_relids)) {
                // Transform expression for child joinrel
                Expr *child_expr;
                if (parent_joinrel->reloptkind == RELOPT_JOINREL) {
                    // Simple single-level transformation
                    child_expr = (Expr *) adjust_appendrel_attrs(root,
                                                               (Node *) cur_em->em_expr,
                                                               nappinfos, appinfos);
                } else {
                    // Multi-level transformation
                    child_expr = (Expr *) adjust_appendrel_attrs_multilevel(root,
                                                                          (Node *) cur_em->em_expr,
                                                                          child_joinrel,
                                                                          child_joinrel->top_parent);
                }

                // Update relids to reference child instead of parent
                Relids new_relids = bms_difference(cur_em->em_relids, top_parent_relids);
                new_relids = bms_add_members(new_relids, child_relids);

                // Add transformed member to equivalence class
                add_eq_member(cur_ec, child_expr, new_relids,
                            cur_em->em_jdomain, cur_em, cur_em->em_datatype);
            }
        }
    }

    MemoryContextSwitchTo(oldcontext);
}
```