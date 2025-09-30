# remove_rel_from_query

## Location
[src/backend/optimizer/plan/analyzejoins.c:329-561](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/analyzejoins.c#L329-L561)

## Overview
Completely removes a target relation and its associated outer join from all of the planner's data structures after determining the join is unnecessary.

## Definition

```c
union(sjinfo->min_lefthand, sjinfo->min_righthand);
```
## Detailed Description
This function performs the comprehensive cleanup required when a join has been determined to be removable by join_is_removable(). It systematically removes all references to both the target relation (relid) and the outer join (ojrelid) from various planner data structures to ensure the eliminated join doesn't interfere with subsequent planning phases.

The function performs cleanup across multiple data structures:

1. **Attribute Dependencies**: Removes references from other relations' attr_needed arrays
2. **Global Relation Sets**: Updates all_baserels, outer_join_rels, and all_query_rels
3. **SpecialJoinInfo Structures**: Updates lefthand/righthand relid sets for nested outer joins
4. **PlaceHolderVar Management**: Either removes PHVs entirely or updates their eval_at and needed sets
5. **Join Qualifiers**: Removes or redistributes join clauses, handling pushed-down conditions appropriately
6. **Equivalence Classes**: Removes relation references from equivalence class structures
7. **Base Relation Array**: Nullifies the relation entry and frees the RelOptInfo structure

The function is designed to be conservative and only updates parts of the planner's data structures that will actually be consulted later in the planning process.

## Parameters / Member Variables
- : PlannerInfo structure containing all planner state and context
- : The ID of the base relation being removed from the query
- : SpecialJoinInfo structure describing the outer join being eliminated

## Dependencies
- Functions called/Symbols referenced:
  - [find_base_rel](../f/find_base_rel.md): Locates the RelOptInfo for the target relation
  - [bms_union](../b/bms_union.md), bms_add_member, bms_del_member: Bitmap set manipulation functions
  - [bms_copy](../b/bms_copy.md): Creates copies of bitmap sets to avoid modifying shared structures
  - [bms_is_member](../b/bms_is_member.md), bms_is_subset, bms_is_empty: Bitmap set query functions
  - foreach_delete_current: Safe deletion during list iteration
  - [remove_join_clause_from_rels](remove_join_clause_from_rels.md): Removes join clauses from relation join lists
  - [remove_rel_from_restrictinfo](remove_rel_from_restrictinfo.md): Updates RestrictInfo relation references
  - [distribute_restrictinfo_to_rels](../d/distribute_restrictinfo_to_rels.md): Redistributes join clauses after modification
  - [remove_rel_from_eclass](remove_rel_from_eclass.md): Removes relation from equivalence classes
  - [pull_varnos](../p/pull_varnos.md): Extracts variable relation IDs for debugging assertions
  - RINFO_IS_PUSHED_DOWN: Macro to identify pushed-down restrictions

- Called from (representative examples):
  - [remove_useless_joins](remove_useless_joins.md): Main join elimination function

## Notes and Other Information
- The function is not "terribly thorough" by design - it only updates data structures that will be consulted later in planning
- Special care is taken with PlaceHolderVars, distinguishing between those used at partner relations versus those used in join qualifiers
- The function handles cloned join clauses that may result from outer join commutation rules
- [SpecialJoinInfo](../S/SpecialJoinInfo.md) structures require copying before modification since they may share relid sets with other structures
- The target relation's RelOptInfo is freed at the end to prevent any further access
- Includes debug assertions to verify that redistributed clauses don't reference the eliminated relation
- Foreign key references are left for match_foreign_keys_to_quals() to clean up later

## Simplified Source

```c
static void remove_rel_from_query(PlannerInfo *root, int relid, SpecialJoinInfo *sjinfo)
{
    RelOptInfo *rel = find_base_rel(root, relid);
    int ojrelid = sjinfo->ojrelid;

    // Calculate join relation set
    joinrelids = bms_union(sjinfo->min_lefthand, sjinfo->min_righthand);
    joinrelids = bms_add_member(joinrelids, ojrelid);

    // Remove references from other baserels' attr_needed arrays
    for (rti = 1; rti < root->simple_rel_array_size; rti++) {
        RelOptInfo *otherrel = root->simple_rel_array[rti];
        if (otherrel == NULL || otherrel == rel)
            continue;

        // Clean attr_needed arrays
        for (attroff = otherrel->max_attr - otherrel->min_attr; attroff >= 0; attroff--) {
            otherrel->attr_needed[attroff] = bms_del_member(otherrel->attr_needed[attroff], relid);
            otherrel->attr_needed[attroff] = bms_del_member(otherrel->attr_needed[attroff], ojrelid);
        }
    }

    // Update global relation sets
    root->all_baserels = bms_del_member(root->all_baserels, relid);
    root->outer_join_rels = bms_del_member(root->outer_join_rels, ojrelid);
    root->all_query_rels = bms_del_member(root->all_query_rels, relid);
    root->all_query_rels = bms_del_member(root->all_query_rels, ojrelid);

    // Update SpecialJoinInfo structures for nested outer joins
    foreach(l, root->join_info_list) {
        SpecialJoinInfo *sjinf = (SpecialJoinInfo *) lfirst(l);

        // Copy to avoid modifying shared structures, then remove references
        sjinf->min_lefthand = bms_del_member(bms_copy(sjinf->min_lefthand), relid);
        sjinf->min_righthand = bms_del_member(bms_copy(sjinf->min_righthand), relid);
        /* similar updates for syn_lefthand, syn_righthand, commute_* fields */
    }

    // Handle PlaceHolderVars: remove entirely or update eval_at/needed sets
    foreach(l, root->placeholder_list) {
        PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(l);

        if (bms_is_subset(phinfo->ph_needed, joinrelids) &&
            bms_is_member(relid, phinfo->ph_eval_at) &&
            !bms_is_member(ojrelid, phinfo->ph_eval_at)) {
            // Remove PHV entirely
            root->placeholder_list = foreach_delete_current(root->placeholder_list, l);
            root->placeholder_array[phinfo->phid] = NULL;
        } else {
            // Update PHV references
            phinfo->ph_eval_at = bms_del_member(phinfo->ph_eval_at, relid);
            phinfo->ph_eval_at = bms_del_member(phinfo->ph_eval_at, ojrelid);
            phinfo->ph_needed = bms_del_member(phinfo->ph_needed, relid);
            phinfo->ph_needed = bms_del_member(phinfo->ph_needed, ojrelid);
        }
    }

    // Remove/redistribute join clauses
    joininfos = list_copy(rel->joininfo);
    foreach(l, joininfos) {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

        remove_join_clause_from_rels(root, rinfo, rinfo->required_relids);

        if (RINFO_IS_PUSHED_DOWN(rinfo, join_plus_commute)) {
            remove_rel_from_restrictinfo(rinfo, relid, ojrelid);
            distribute_restrictinfo_to_rels(root, rinfo);
        }
    }

    // Remove from EquivalenceClasses
    foreach(l, root->eq_classes) {
        EquivalenceClass *ec = (EquivalenceClass *) lfirst(l);
        if (bms_is_member(relid, ec->ec_relids) || bms_is_member(ojrelid, ec->ec_relids))
            remove_rel_from_eclass(ec, relid, ojrelid);
    }

    // Clean up: nullify relation entry and free memory
    root->simple_rel_array[relid] = NULL;
    pfree(rel);
}
```