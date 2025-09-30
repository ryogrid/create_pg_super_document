# join_search_one_level

## Location
[src/backend/optimizer/path/joinrels.c:73-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L73-L279)

## Overview
A core function in PostgreSQL's dynamic programming join search algorithm that considers all ways to produce join relations containing exactly 'level' jointree items in one step of the optimization process.

## Definition

```c
void
join_search_one_level(PlannerInfo *root, int level)
```
## Detailed Description
The  function is a fundamental component of PostgreSQL's standard join search algorithm. It implements one step of the dynamic programming method used to find optimal join orders. The function systematically generates all feasible join combinations at a specific level by:

1. **Left-sided and right-sided plans**: Joining relations of exactly (level-1) members against initial relations, preferring joins with join clauses over Cartesian products
2. **Bushy plans**: Considering joins between relations of k initial rels with relations of (level-k) initial rels for intermediate values of k
3. **Fallback Cartesian products**: As a last resort, generating Cartesian product joins when no clause-based joins are possible

The function uses join clauses, equivalence classes, and join order restrictions to guide the search and avoid generating unreasonable numbers of join combinations.

## Parameters / Member Variables
- : PlannerInfo structure containing the query planning context and join relation levels
- : The target level (number of relations) for joins to be created in this iteration

## Dependencies
- Functions called/Symbols referenced:
  - [has_join_restriction](../h/has_join_restriction.md)
  - foreach_current_index
  - [make_rels_by_clause_joins](../m/make_rels_by_clause_joins.md)
  - [make_rels_by_clauseless_joins](../m/make_rels_by_clauseless_joins.md)
  - for_each_from
  - [bms_overlap](../b/bms_overlap.md)
  - [have_relevant_joinclause](../h/have_relevant_joinclause.md)
  - [have_join_order_restriction](../h/have_join_order_restriction.md)
  - [make_join_rel](../m/make_join_rel.md)
- Called from (representative examples):
  - [standard_join_search](../s/standard_join_search.md)

## Notes and Other Information
- The function modifies  to ensure new joinrels are added to the proper list
- Results are stored in 
- Special handling exists for sub-joinlist scenarios where all relations have only external join clauses
- Includes sanity checking to detect cases where no legal joins can be formed when no special joins or lateral references exist
- The algorithm avoids duplicate work by leveraging symmetry in join operations and careful iteration bounds

## Simplified Source

```c
void join_search_one_level(PlannerInfo *root, int level) {
    List **joinrels = root->join_rel_level;
    ListCell *r;
    int k;

    // Set current level for new join relations
    root->join_cur_level = level;

    // Phase 1: Left-sided and right-sided plans
    // Join (level-1) relations against initial relations
    foreach(r, joinrels[level - 1]) {
        RelOptInfo *old_rel = (RelOptInfo *) lfirst(r);

        if (old_rel->joininfo != NIL || old_rel->has_eclass_joins ||
            has_join_restriction(root, old_rel)) {
            // Has join clauses - make selective joins
            int first_rel = (level == 2) ? foreach_current_index(r) + 1 : 0;
            make_rels_by_clause_joins(root, old_rel, joinrels[1], first_rel);
        } else {
            // No join clauses - make Cartesian products
            make_rels_by_clauseless_joins(root, old_rel, joinrels[1]);
        }
    }

    // Phase 2: Bushy plans
    // Join k-way relations with (level-k)-way relations
    for (k = 2; k <= level/2; k++) {
        int other_level = level - k;

        foreach(r, joinrels[k]) {
            RelOptInfo *old_rel = (RelOptInfo *) lfirst(r);

            // Skip relations without join clauses or restrictions
            if (old_rel->joininfo == NIL && !old_rel->has_eclass_joins &&
                !has_join_restriction(root, old_rel))
                continue;

            int first_rel = (k == other_level) ? foreach_current_index(r) + 1 : 0;

            // Try joining with relations at other_level
            ListCell *r2;
            for_each_from(r2, joinrels[other_level], first_rel) {
                RelOptInfo *new_rel = (RelOptInfo *) lfirst(r2);

                // Check relations don't overlap and have join conditions
                if (!bms_overlap(old_rel->relids, new_rel->relids) &&
                    (have_relevant_joinclause(root, old_rel, new_rel) ||
                     have_join_order_restriction(root, old_rel, new_rel))) {
                    make_join_rel(root, old_rel, new_rel);
                }
            }
        }
    }

    // Phase 3: Last resort - force Cartesian products if no joins found
    if (joinrels[level] == NIL) {
        foreach(r, joinrels[level - 1]) {
            RelOptInfo *old_rel = (RelOptInfo *) lfirst(r);
            make_rels_by_clauseless_joins(root, old_rel, joinrels[1]);
        }

        // Sanity check for cases without special joins
        if (joinrels[level] == NIL &&
            root->join_info_list == NIL && !root->hasLateralRTEs) {
            elog(ERROR, "failed to build any %d-way joins", level);
        }
    }
}
```