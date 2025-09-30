# reduce_unique_semijoins

## Location
[src/backend/optimizer/plan/analyzejoins.c:730-805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/analyzejoins.c#L730-L805)

## Overview
Optimizes semijoins by converting them to plain inner joins when the inner relation is provably unique for the join clauses, eliminating unnecessary semijoin overhead.

## Definition
```c
void reduce_unique_semijoins(PlannerInfo *root)
```

## Detailed Description
This function performs a query optimization by identifying semijoins that can be safely converted to inner joins. The transformation is valid when the inner relation of a semijoin is guaranteed to be unique for the join conditions, meaning each row from the outer relation will match at most one row from the inner relation.

The function works by:
1. Scanning the join_info_list to identify semijoin operations
2. Checking if the semijoin has a single base relation on the right-hand side
3. Verifying that the inner relation supports distinctness analysis
4. Computing the relevant join clauses (both explicit and EC-derived)
5. Testing whether the inner relation is unique for those clauses
6. If uniqueness is proven, removing the SpecialJoinInfo to allow the semijoin to be treated as an inner join

This optimization can significantly improve query performance by enabling more efficient join algorithms and removing the need for duplicate elimination that semijoins typically require.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo structure containing query planning information, including the join_info_list to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md) (checks for single-member bitmapset)
  - [find_base_rel](../f/find_base_rel.md) (locates base relation information)
  - [rel_supports_distinctness](rel_supports_distinctness.md) (checks if relation supports uniqueness analysis)
  - [bms_union](../b/bms_union.md) (combines bitmapsets)
  - [list_concat](../l/list_concat.md) (concatenates lists)
  - [generate_join_implied_equalities](../g/generate_join_implied_equalities.md) (creates implied join conditions from equivalence classes)
  - [innerrel_is_unique](../i/innerrel_is_unique.md) (tests uniqueness of inner relation for given conditions)
  - foreach_delete_current (removes current list element during iteration)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md)

## Notes and Other Information
- This optimization happens after reduce_outer_joins because sufficient information for uniqueness analysis is not available during that earlier phase
- The function only considers semijoins to single base relations, as multi-relation right-hand sides are too complex for this analysis
- When a semijoin is reduced, only the SpecialJoinInfo is removed; the join type in the query jointree is left unchanged since it won't be consulted again
- The function processes both explicit join clauses and equivalence class-derived join clauses
- This is a global optimization function called during the main query planning phase
- Located in src/backend/optimizer/plan/analyzejoins.c at lines 730-805

## Simplified Source

```c
void reduce_unique_semijoins(PlannerInfo *root) {
    ListCell *lc;

    // Scan through all special joins to find semijoins
    foreach(lc, root->join_info_list) {
        SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);
        int innerrelid;
        RelOptInfo *innerrel;
        Relids joinrelids;
        List *restrictlist;

        // Only process semijoins to single base relations
        if (sjinfo->jointype != JOIN_SEMI)
            continue;

        if (!bms_get_singleton_member(sjinfo->min_righthand, &innerrelid))
            continue;

        innerrel = find_base_rel(root, innerrelid);

        // Quick check: can this relation support uniqueness analysis?
        if (!rel_supports_distinctness(root, innerrel))
            continue;

        // Compute the complete join relid set
        joinrelids = bms_union(sjinfo->min_lefthand, sjinfo->min_righthand);

        // Collect all relevant join clauses (explicit + EC-derived)
        restrictlist = list_concat(
            generate_join_implied_equalities(root, joinrelids,
                                             sjinfo->min_lefthand,
                                             innerrel, NULL),
            innerrel->joininfo);

        // Test if the inner relation is unique for these join clauses
        if (!innerrel_is_unique(root, joinrelids, sjinfo->min_lefthand,
                                innerrel, JOIN_SEMI, restrictlist, true))
            continue;

        // Remove the semijoin - it can be treated as an inner join
        root->join_info_list = foreach_delete_current(root->join_info_list, lc);
    }
}
```