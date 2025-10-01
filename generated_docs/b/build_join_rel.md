# build_join_rel

## Location
[src/backend/optimizer/util/relnode.c:665-880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L665-L880)

## Overview
Creates and initializes a RelOptInfo structure representing the join of two relations, handling both the creation of new join relations and retrieval of existing ones.

## Definition

```c
union(outer_rel->direct_lateral_relids,
				  inner_rel->direct_lateral_relids);
```
## Detailed Description
This function is the central hub for creating join relations in PostgreSQL's query optimizer. It first checks if a join relation for the given set of base relations already exists using . If found, it only needs to compute the restrictlist for the specific pair of relations. If not found, it creates a new  node of type  and initializes all its fields.

The function performs several critical tasks:
1. Creates and initializes a new join relation structure
2. Computes lateral parameterization using 
3. Builds the target list by calling  for both outer and inner relations
4. Constructs restrict and join clause lists
5. Sets size estimates for the join relation
6. Determines parallel execution feasibility
7. Adds the join relation to the planner's data structures

## Parameters / Member Variables
- : PlannerInfo containing global planner state and context
- : Relids set uniquely identifying the join relation
- : RelOptInfo for the outer relation to be joined
- : RelOptInfo for the inner relation to be joined  
- : SpecialJoinInfo containing join context and constraints
- : List of any pushed-down outer joins that are now completed
- : Output parameter receiving the list of RestrictInfo nodes for this join pair

## Dependencies
- Functions called/Symbols referenced:
  - [find_join_rel](../f/find_join_rel.md)
  - build_joinrel_restrictlist
  - min_join_parameterization
  - build_joinrel_tlist
  - [add_placeholders_to_joinrel](../a/add_placeholders_to_joinrel.md)
  - [build_joinrel_joinlist](build_joinrel_joinlist.md)
  - [has_relevant_eclass_joinclause](../h/has_relevant_eclass_joinclause.md)
  - [build_joinrel_partition_info](build_joinrel_partition_info.md)
  - [set_joinrel_size_estimates](../s/set_joinrel_size_estimates.md)
  - [set_foreign_rel_properties](../s/set_foreign_rel_properties.md)
  - [add_join_rel](../a/add_join_rel.md)
- Called from (representative examples):
  - [make_join_rel](../m/make_join_rel.md)

## Notes and Other Information
- This function should only be used for joins between parent relations (not other rel types)
- The target list order for a join relation depends on which pair of outer/inner relations is first used to build it, but the contents remain consistent
- The function handles parallel execution considerations by checking if both input relations support parallel execution and if all expressions are parallel-safe
- The restrictlist_ptr parameter makes the API somewhat awkward but avoids duplicated restrictlist calculations
- For dynamic-programming join search, the new join relation is added to the appropriate level sublist

## Simplified Source

```c
RelOptInfo *build_join_rel(PlannerInfo *root, Relids joinrelids,
                          RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                          SpecialJoinInfo *sjinfo, List *pushed_down_joins,
                          List **restrictlist_ptr) {
    RelOptInfo *joinrel;
    List *restrictlist;

    // Check if join relation already exists
    joinrel = find_join_rel(root, joinrelids);
    if (joinrel) {
        // Just compute restrictlist for this specific pair
        if (restrictlist_ptr)
            *restrictlist_ptr = build_joinrel_restrictlist(root, joinrel, outer_rel, inner_rel, sjinfo);
        return joinrel;
    }

    // Create new join relation
    joinrel = makeNode(RelOptInfo);
    joinrel->reloptkind = RELOPT_JOINREL;
    joinrel->relids = bms_copy(joinrelids);
    joinrel->consider_startup = (root->tuple_fraction > 0);
    joinrel->reltarget = create_empty_pathtarget();

    // Initialize all list and pointer fields to NULL/NIL
    joinrel->pathlist = NIL;
    joinrel->partial_pathlist = NIL;
    joinrel->cheapest_startup_path = NULL;
    joinrel->cheapest_total_path = NULL;
    joinrel->lateral_vars = NIL;
    joinrel->indexlist = NIL;
    joinrel->baserestrictinfo = NIL;
    joinrel->joininfo = NIL;

    // Compute lateral parameterization
    joinrel->direct_lateral_relids = bms_union(outer_rel->direct_lateral_relids, inner_rel->direct_lateral_relids);
    joinrel->lateral_relids = min_join_parameterization(root, joinrel->relids, outer_rel, inner_rel);

    // Set foreign relation properties
    set_foreign_rel_properties(joinrel, outer_rel, inner_rel);

    // Build target list from both relations
    build_joinrel_tlist(root, joinrel, outer_rel, sjinfo, pushed_down_joins, (sjinfo->jointype == JOIN_FULL));
    build_joinrel_tlist(root, joinrel, inner_rel, sjinfo, pushed_down_joins, (sjinfo->jointype != JOIN_INNER));
    add_placeholders_to_joinrel(root, joinrel, outer_rel, inner_rel, sjinfo);

    // Build restrictlist and joinlist
    restrictlist = build_joinrel_restrictlist(root, joinrel, outer_rel, inner_rel, sjinfo);
    if (restrictlist_ptr)
        *restrictlist_ptr = restrictlist;
    build_joinrel_joinlist(joinrel, outer_rel, inner_rel);

    // Check for equivalence class joins
    joinrel->has_eclass_joins = has_relevant_eclass_joinclause(root, joinrel);

    // Set up partitioning info and size estimates
    build_joinrel_partition_info(root, joinrel, outer_rel, inner_rel, sjinfo, restrictlist);
    set_joinrel_size_estimates(root, joinrel, outer_rel, inner_rel, sjinfo, restrictlist);

    // Determine parallel execution feasibility
    if (inner_rel->consider_parallel && outer_rel->consider_parallel &&
        is_parallel_safe(root, (Node *) restrictlist) &&
        is_parallel_safe(root, (Node *) joinrel->reltarget->exprs))
        joinrel->consider_parallel = true;

    // Add to planner data structures
    add_join_rel(root, joinrel);
    if (root->join_rel_level) {
        root->join_rel_level[root->join_cur_level] =
            lappend(root->join_rel_level[root->join_cur_level], joinrel);
    }

    return joinrel;
}
```