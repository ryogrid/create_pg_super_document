# add_paths_to_joinrel

## Location
[src/backend/optimizer/path/joinpath.c:124-362](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L124-L362)

## Overview
Considers all possible join paths between two component relations and adds the best paths to the join relation's pathlist, serving as the main driver for join path generation in PostgreSQL's query optimizer.

## Definition

```c
void
add_paths_to_joinrel(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 RelOptInfo *outerrel,
					 RelOptInfo *innerrel,
					 JoinType jointype,
					 SpecialJoinInfo *sjinfo,
					 List *restrictlist)
```
## Detailed Description
This function is the central hub for generating all types of join paths between two relations in PostgreSQL's cost-based optimizer. It systematically evaluates different join algorithms (nested loop, merge join, hash join) and path configurations to find the most efficient ways to combine the outer and inner relations.

The function performs several key operations:
1. Determines if the inner relation is provably unique for cost estimation optimizations
2. Identifies potential merge join clauses when merge joins are enabled
3. Computes correction factors for semi/anti joins and unique joins
4. Establishes parameterization constraints based on join ordering restrictions
5. Generates paths using various join algorithms (sort-merge, nested loop, hash)
6. Allows foreign data wrappers and extensions to contribute additional paths

The function handles special join types including semi-joins, anti-joins, and unique joins with appropriate logic for each case.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state and configuration
- : Target RelOptInfo representing the result of joining outerrel and innerrel
- : RelOptInfo for the outer (left) side of the join
- : RelOptInfo for the inner (right) side of the join  
- : JoinType specifying the type of join (INNER, LEFT, RIGHT, FULL, SEMI, ANTI, etc.)
- : SpecialJoinInfo containing join ordering constraints and metadata
- : List of RestrictInfo nodes representing join conditions

## Dependencies
- Functions called/Symbols referenced:
  - [innerrel_is_unique](../i/innerrel_is_unique.md)
  - [select_mergejoin_clauses](../s/select_mergejoin_clauses.md)
  - [compute_semi_anti_join_factors](../c/compute_semi_anti_join_factors.md)
  - [sort_inner_and_outer](../s/sort_inner_and_outer.md)
  - [match_unsorted_outer](../m/match_unsorted_outer.md)
  - [hash_inner_and_outer](../h/hash_inner_and_outer.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_join](../b/bms_join.md)
  - [bms_difference](../b/bms_difference.md)
  - [bms_add_members](../b/bms_add_members.md)
- Called from (representative examples):
  - [populate_joinrel_with_paths](../p/populate_joinrel_with_paths.md)

## Notes and Other Information
The function supports special JoinTypes JOIN_UNIQUE_OUTER and JOIN_UNIQUE_INNER which indicate that a relation should be unique-ified before applying a regular inner join. These values are internal to this module and don't propagate outside.

The function includes logic to handle partitioned tables by using top_parent_relids for RELOPT_OTHER_JOINREL relations. It also considers LATERAL subquery dependencies when determining parameterization constraints.

For full outer joins, the function overrides disabled join methods (merge join, hash join) since they may be the only feasible implementation approach.

## Simplified Source

```c
void
add_paths_to_joinrel(PlannerInfo *root,
                     RelOptInfo *joinrel,
                     RelOptInfo *outerrel,
                     RelOptInfo *innerrel,
                     JoinType jointype,
                     SpecialJoinInfo *sjinfo,
                     List *restrictlist)
{
    JoinPathExtraData extra;
    bool mergejoin_allowed = true;
    Relids joinrelids;

    // Setup join relation identifiers for partitioned tables
    if (joinrel->reloptkind == RELOPT_OTHER_JOINREL)
        joinrelids = joinrel->top_parent_relids;
    else
        joinrelids = joinrel->relids;

    // Initialize extra data for path generation
    extra.restrictlist = restrictlist;
    extra.mergeclause_list = NIL;
    extra.sjinfo = sjinfo;
    extra.param_source_rels = NULL;

    // Determine if inner relation is unique for this join
    switch (jointype)
    {
        case JOIN_SEMI:
        case JOIN_ANTI:
            extra.inner_unique = false; // Not proven for these types
            break;
        case JOIN_UNIQUE_INNER:
            extra.inner_unique = bms_is_subset(sjinfo->min_lefthand, outerrel->relids);
            break;
        case JOIN_UNIQUE_OUTER:
            extra.inner_unique = innerrel_is_unique(root, joinrel->relids, outerrel->relids,
                                                   innerrel, JOIN_INNER, restrictlist, false);
            break;
        default:
            extra.inner_unique = innerrel_is_unique(root, joinrel->relids, outerrel->relids,
                                                   innerrel, jointype, restrictlist, false);
            break;
    }

    // Find mergejoin clauses if merge joins are enabled
    if (enable_mergejoin || jointype == JOIN_FULL)
        extra.mergeclause_list = select_mergejoin_clauses(root, joinrel, outerrel, innerrel,
                                                         restrictlist, jointype, &mergejoin_allowed);

    // Compute cost factors for semi/anti joins
    if (jointype == JOIN_SEMI || jointype == JOIN_ANTI || extra.inner_unique)
        compute_semi_anti_join_factors(root, joinrel, outerrel, innerrel,
                                      jointype, sjinfo, restrictlist, &extra.semifactors);

    // Determine parameterization constraints from join ordering restrictions
    foreach(lc, root->join_info_list)
    {
        SpecialJoinInfo *sjinfo2 = (SpecialJoinInfo *) lfirst(lc);

        // Add constraints for joins that overlap with this join's RHS
        if (bms_overlap(joinrelids, sjinfo2->min_righthand) &&
            !bms_overlap(joinrelids, sjinfo2->min_lefthand))
            extra.param_source_rels = bms_join(extra.param_source_rels,
                                              bms_difference(root->all_baserels, sjinfo2->min_righthand));
    }

    // Add lateral dependencies to parameterization
    extra.param_source_rels = bms_add_members(extra.param_source_rels, joinrel->lateral_relids);

    // Generate different types of join paths:

    // 1. Mergejoin paths where both relations are sorted
    if (mergejoin_allowed)
        sort_inner_and_outer(root, joinrel, outerrel, innerrel, jointype, &extra);

    // 2. Nested loop and mergejoin paths with unsorted outer relation
    if (mergejoin_allowed)
        match_unsorted_outer(root, joinrel, outerrel, innerrel, jointype, &extra);

    // 3. Hash join paths
    if (enable_hashjoin || jointype == JOIN_FULL)
        hash_inner_and_outer(root, joinrel, outerrel, innerrel, jointype, &extra);

    // 4. Foreign table join paths (if applicable)
    if (joinrel->fdwroutine && joinrel->fdwroutine->GetForeignJoinPaths)
        joinrel->fdwroutine->GetForeignJoinPaths(root, joinrel, outerrel, innerrel, jointype, &extra);

    // 5. Extension-provided paths
    if (set_join_pathlist_hook)
        set_join_pathlist_hook(root, joinrel, outerrel, innerrel, jointype, &extra);
}
```