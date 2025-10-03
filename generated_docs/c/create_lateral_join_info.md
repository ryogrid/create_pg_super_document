# create_lateral_join_info

## Location
[src/backend/optimizer/plan/initsplan.c:501-739](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L501-L739)

## Overview
Analyzes and establishes lateral dependency relationships between base relations by computing direct and transitive lateral reference sets.

## Definition

```c
void
create_lateral_join_info(PlannerInfo *root)
```
## Detailed Description
This function is responsible for building the complete picture of lateral dependencies in a query by examining all base relations and establishing three key sets for each relation:
- **direct_lateral_relids**: Relations directly referenced by LATERAL constructs
- **lateral_relids**: All relations that must be available (direct and indirect dependencies) 
- **lateral_referencers**: Relations that reference this relation laterally

The function operates in several phases:
1. Processes simple lateral references from variables extracted by extract_lateral_references
2. Handles lateral references within PlaceHolderVars, considering their evaluation sites
3. Computes the transitive closure using Warshall's algorithm to capture indirect dependencies
4. Creates reverse mapping to identify which relations are referenced by others

This comprehensive analysis is essential for join ordering, as relations with lateral dependencies must be processed in the correct sequence during plan generation.

## Parameters / Member Variables
- `*root`: The PlannerInfo structure containing the query tree and planning state
## Dependencies
- Functions called/Symbols referenced:
  - [find_placeholder_info](../f/find_placeholder_info.md)
  - [find_base_rel](../f/find_base_rel.md)
  - [find_base_rel_ignore_join](../f/find_base_rel_ignore_join.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_add_members](../b/bms_add_members.md)
  - [bms_copy](../b/bms_copy.md)
  - [bms_intersect](../b/bms_intersect.md)
  - bms_is_empty
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_next_member](../b/bms_next_member.md)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md)

## Notes and Other Information
- Only executes if root->hasLateralRTEs is true, providing early exit optimization
- Requires that root->placeholdersFrozen is true to ensure PlaceHolderVar evaluation sites are finalized
- Handles different evaluation scenarios for PlaceHolderVars (baserel vs join evaluation sites)
- Uses transitive closure computation to ensure all indirect lateral dependencies are captured
- Filters lateral references to include only base relations, excluding outer joins from dependency tracking
- Resets root->hasLateralRTEs to false if no actual lateral references are found, optimizing subsequent processing
- The lateral_referencers set enables efficient reverse lookup during join planning

## Simplified Source

```c
void create_lateral_join_info(PlannerInfo *root)
{
    bool found_laterals = false;
    Index rti;

    // Early exit if query contains no LATERAL RTEs
    if (!root->hasLateralRTEs)
        return;

    Assert(root->placeholdersFrozen);

    // Phase 1: Process direct lateral references for each baserel
    for (rti = 1; rti < root->simple_rel_array_size; rti++) {
        RelOptInfo *brel = root->simple_rel_array[rti];
        Relids lateral_relids = NULL;

        if (brel == NULL || brel->reloptkind != RELOPT_BASEREL)
            continue;

        // Extract lateral references from variables and PlaceHolderVars
        foreach(lc, brel->lateral_vars) {
            Node *node = (Node *) lfirst(lc);

            if (IsA(node, Var)) {
                Var *var = (Var *) node;
                found_laterals = true;
                lateral_relids = bms_add_member(lateral_relids, var->varno);
            } else if (IsA(node, PlaceHolderVar)) {
                PlaceHolderVar *phv = (PlaceHolderVar *) node;
                PlaceHolderInfo *phinfo = find_placeholder_info(root, phv);
                found_laterals = true;
                lateral_relids = bms_add_members(lateral_relids, phinfo->ph_eval_at);
            }
        }

        brel->direct_lateral_relids = lateral_relids;
        brel->lateral_relids = bms_copy(lateral_relids);
    }

    // Phase 2: Process PlaceHolderVar lateral references
    foreach(lc, root->placeholder_list) {
        PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(lc);
        Relids lateral_refs;

        if (phinfo->ph_lateral == NULL)
            continue;

        found_laterals = true;
        lateral_refs = bms_intersect(phinfo->ph_lateral, root->all_baserels);

        if (bms_get_singleton_member(phinfo->ph_eval_at, &varno)) {
            // Evaluation site is a baserel
            RelOptInfo *brel = find_base_rel(root, varno);
            brel->direct_lateral_relids = bms_add_members(brel->direct_lateral_relids, lateral_refs);
            brel->lateral_relids = bms_add_members(brel->lateral_relids, lateral_refs);
        } else {
            // Evaluation site is a join - add to all member baserels
            varno = -1;
            while ((varno = bms_next_member(phinfo->ph_eval_at, varno)) >= 0) {
                RelOptInfo *brel = find_base_rel_ignore_join(root, varno);
                if (brel != NULL)
                    brel->lateral_relids = bms_add_members(brel->lateral_relids, lateral_refs);
            }
        }
    }

    // Early exit if no actual lateral references found
    if (!found_laterals) {
        root->hasLateralRTEs = false;
        return;
    }

    // Phase 3: Compute transitive closure using Warshall's algorithm
    for (rti = 1; rti < root->simple_rel_array_size; rti++) {
        RelOptInfo *brel = root->simple_rel_array[rti];
        Relids outer_lateral_relids;

        if (brel == NULL || brel->reloptkind != RELOPT_BASEREL)
            continue;

        outer_lateral_relids = brel->lateral_relids;
        if (outer_lateral_relids == NULL)
            continue;

        // Propagate lateral dependencies to relations that reference this one
        for (Index rti2 = 1; rti2 < root->simple_rel_array_size; rti2++) {
            RelOptInfo *brel2 = root->simple_rel_array[rti2];
            if (brel2 == NULL || brel2->reloptkind != RELOPT_BASEREL)
                continue;

            if (bms_is_member(rti, brel2->lateral_relids))
                brel2->lateral_relids = bms_add_members(brel2->lateral_relids, outer_lateral_relids);
        }
    }

    // Phase 4: Build inverse mapping (lateral_referencers)
    for (rti = 1; rti < root->simple_rel_array_size; rti++) {
        RelOptInfo *brel = root->simple_rel_array[rti];
        Relids lateral_relids;

        if (brel == NULL || brel->reloptkind != RELOPT_BASEREL)
            continue;

        lateral_relids = brel->lateral_relids;
        if (bms_is_empty(lateral_relids))
            continue;

        // Mark this relation as a referencer of its dependencies
        int rti2 = -1;
        while ((rti2 = bms_next_member(lateral_relids, rti2)) >= 0) {
            RelOptInfo *brel2 = root->simple_rel_array[rti2];
            if (brel2 != NULL)
                brel2->lateral_referencers = bms_add_member(brel2->lateral_referencers, rti);
        }
    }
}
```