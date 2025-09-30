# deconstruct_distribute_oj_quals

## Location
[src/backend/optimizer/plan/initsplan.c:1878-2118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L1878-L2118)

## Overview
Adjusts LEFT JOIN qualifiers for commuted-left-join cases and distributes them into the appropriate joinqual lists and EquivalenceClass structures.

## Definition

```c
static void
deconstruct_distribute_oj_quals(PlannerInfo *root,
								List *jtitems,
								JoinTreeItem *jtitem)
```
## Detailed Description
The  function handles the complex task of processing postponed outer join qualifiers after the main deconstruct_distribute scan is complete. This function is critical for implementing outer join identity optimizations, particularly identity 3, which allows certain LEFT JOINs to commute under specific conditions.

The function performs several key operations:
1. Recomputes syntactic and semantic scopes for the current left join
2. Determines if the join can commute with other joins based on outer join identity rules
3. Generates multiple variants of join clauses with different nullingrels labeling when commutation is possible
4. Distributes the processed qualifiers to appropriate relation structures and creates EquivalenceClasses

When commutation is possible, the function creates different versions of the join conditions corresponding to various join orderings that are semantically equivalent. This enables the optimizer to consider more execution plans while maintaining correctness of NULL-generation semantics.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning state and optimizer information
- : Complete list of JoinTreeItems in depth-first order from the deconstruct scan
- : Specific JoinTreeItem containing postponed oj_joinclauses that need processing

## Dependencies
- Functions called/Symbols referenced:
  - [bms_union](../b/bms_union.md), bms_add_member, bms_del_member, bms_make_singleton (bitmap operations)
  - [remove_nulling_relids](../r/remove_nulling_relids.md)
  - [add_nulling_relids](../a/add_nulling_relids.md)
  - [distribute_quals_to_rels](distribute_quals_to_rels.md)
  - [bms_copy](../b/bms_copy.md)
  - [bms_equal](../b/bms_equal.md)
  - [bms_is_member](../b/bms_is_member.md)
  - bms_is_empty
- Called from (representative examples):
  - [deconstruct_jointree](deconstruct_jointree.md)

## Notes and Other Information
- The function only processes joins where lhs_strict is true, as indicated by the assertion
- When generating qual variants for commuting joins, it processes them in syntactic nesting order using the jtitems list
- EquivalenceClasses are generated only from the first form of quals (with fewest nullingrels bits) to avoid creating nonsensical equivalences
- The function implements proper nullingrels bit manipulation to maintain correct NULL semantics when joins are reordered
- Serial number management ensures that RestrictInfos for the "same" qual condition get identical serial numbers for duplicate detection
- The incompatible_joins mechanism prevents quals from being applied at incorrect join levels
- When no commutation is possible, the function simply distributes the postponed clauses as-is without creating variants

## Simplified Source

```c
static void deconstruct_distribute_oj_quals(PlannerInfo *root,
                                           List *jtitems,
                                           JoinTreeItem *jtitem)
{
    SpecialJoinInfo *sjinfo = jtitem->sjinfo;
    Relids qualscope, ojscope, nonnullable_rels;

    // Recompute syntactic and semantic scopes
    qualscope = bms_union(sjinfo->syn_lefthand, sjinfo->syn_righthand);
    qualscope = bms_add_member(qualscope, sjinfo->ojrelid);
    ojscope = bms_union(sjinfo->min_lefthand, sjinfo->min_righthand);
    nonnullable_rels = sjinfo->syn_lefthand;

    // Check if this join can commute with others
    if (sjinfo->commute_above_r || sjinfo->commute_below_l) {
        Relids joins_above = sjinfo->commute_above_r;
        Relids joins_below = sjinfo->commute_below_l;
        Relids incompatible_joins;
        Relids joins_so_far;
        List *quals;
        int save_last_rinfo_serial;
        ListCell *lc;

        // Start with stripped quals (remove lower commuting join nulling bits)
        quals = jtitem->oj_joinclauses;
        if (!bms_is_empty(joins_below))
            quals = (List *) remove_nulling_relids((Node *) quals,
                                                  joins_below, NULL);

        incompatible_joins = bms_union(joins_below, joins_above);
        incompatible_joins = bms_add_member(incompatible_joins, sjinfo->ojrelid);

        save_last_rinfo_serial = root->last_rinfo_serial;
        joins_so_far = NULL;

        // Process each join level in syntactic order
        foreach(lc, jtitems) {
            JoinTreeItem *otherjtitem = (JoinTreeItem *) lfirst(lc);
            SpecialJoinInfo *othersj = otherjtitem->sjinfo;
            bool below_sjinfo = false;
            bool above_sjinfo = false;

            if (othersj == NULL)
                continue;

            // Determine relationship to current join
            if (bms_is_member(othersj->ojrelid, joins_below))
                below_sjinfo = true;
            else if (othersj == sjinfo)
                continue; // Found our join
            else if (bms_is_member(othersj->ojrelid, joins_above))
                above_sjinfo = true;
            else
                continue;

            // Reset serial counter for consistent RestrictInfo numbering
            root->last_rinfo_serial = save_last_rinfo_serial;

            // Adjust nulling bits for joins above
            if (above_sjinfo) {
                quals = (List *) add_nulling_relids((Node *) quals,
                                                   sjinfo->syn_lefthand,
                                                   bms_make_singleton(othersj->ojrelid));
                incompatible_joins = bms_del_member(incompatible_joins,
                                                   othersj->ojrelid);
            }

            // Compute scope for this level
            Relids this_qualscope = bms_union(qualscope, joins_so_far);
            Relids this_ojscope = bms_union(ojscope, joins_so_far);

            if (above_sjinfo) {
                this_qualscope = bms_add_member(this_qualscope, othersj->ojrelid);
                this_ojscope = bms_add_member(this_ojscope, othersj->ojrelid);
                this_ojscope = bms_del_member(this_ojscope, sjinfo->ojrelid);
            }

            // Generate EquivalenceClasses only from first form
            bool allow_equivalence = (joins_so_far == NULL);
            bool has_clone = allow_equivalence;
            bool is_clone = !has_clone;

            // Distribute quals for this join level
            distribute_quals_to_rels(root, quals, otherjtitem, sjinfo,
                                    root->qual_security_level,
                                    this_qualscope, this_ojscope, nonnullable_rels,
                                    bms_copy(incompatible_joins),
                                    allow_equivalence, has_clone, is_clone, NULL);

            // Adjust nulling bits for next level
            if (below_sjinfo) {
                quals = (List *) add_nulling_relids((Node *) quals,
                                                   othersj->syn_righthand,
                                                   bms_make_singleton(othersj->ojrelid));
                incompatible_joins = bms_del_member(incompatible_joins,
                                                   othersj->ojrelid);
            }

            joins_so_far = bms_add_member(joins_so_far, othersj->ojrelid);
        }
    }
    else {
        // No commutation possible - distribute as-is
        distribute_quals_to_rels(root, jtitem->oj_joinclauses,
                                jtitem, sjinfo, root->qual_security_level,
                                qualscope, ojscope, nonnullable_rels,
                                NULL, true, false, false, NULL);
    }
}
```