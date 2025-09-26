# SemiAntiJoinFactors

## Location
src/include/nodes/pathnodes.h: 3211 - 3215

## Overview
SemiAntiJoinFactors contains correction factors used in cost estimation for SEMI, ANTI, and inner_unique joins to account for early termination when the executor stops scanning after finding matches.

## Definition
```c
typedef struct SemiAntiJoinFactors
{
    Selectivity outer_match_frac;
    Selectivity match_count;
} SemiAntiJoinFactors;
```

## Detailed Description
SemiAntiJoinFactors is a specialized data structure used by PostgreSQL's query planner to improve cost estimation accuracy for SEMI joins, ANTI joins, and inner_unique joins. These join types have unique execution characteristics where the executor can terminate inner relation scanning early once specific conditions are met.

For SEMI joins (like EXISTS subqueries), the executor stops scanning the inner relation as soon as it finds the first match for the current outer tuple. For ANTI joins (like NOT EXISTS subqueries), the executor must scan until it either finds a match (and can then reject the outer tuple) or exhausts all inner tuples (confirming no match exists). Inner_unique joins can also benefit from early termination when uniqueness constraints guarantee at most one match.

The factors stored in this structure are computed once per relation pair by compute_semi_anti_join_factors() and then reused across multiple path cost calculations for the same relation pair. This avoids redundant computation since the factors depend only on the selected outer and inner relations, not on the specific access paths chosen for them.

These correction factors are essential for both nested loop and hash join cost estimation, allowing the planner to make more accurate decisions about join method selection and join ordering in queries involving these special join semantics.

## Parameters / Member Variables
- : The fraction (selectivity) of outer relation tuples that are expected to have at least one matching tuple in the inner relation. This value ranges from 0.0 (no outer tuples have matches) to 1.0 (all outer tuples have matches)
- : The average number of matches expected for outer tuples that do have at least one match in the inner relation. This accounts for cases where outer tuples might match multiple inner tuples before early termination occurs

## Dependencies
- Functions called/Symbols referenced:
  - Selectivity (selectivity estimation type)

- Called from (representative examples):
  - compute_semi_anti_join_factors (in costsize.c:5014) - computes these factors
  - JoinPathExtraData (in pathnodes.h:3236) - uses these factors in join path data
  - ConstraintExclusionType (in cost.h:186) - related to cost estimation infrastructure

## Notes and Other Information
- Designed for efficiency by calculating correction factors once per relation pair rather than once per path consideration
- Critical for accurate cost estimation in queries with EXISTS, NOT EXISTS, and similar subquery patterns
- The factors enable the planner to account for early termination benefits when comparing different join algorithms
- Part of PostgreSQL's sophisticated cost estimation framework that helps select optimal execution strategies
- Particularly important for queries where SEMI/ANTI join selectivity significantly impacts overall query performance