# innerrel_is_unique

## Location
src/backend/optimizer/plan/analyzejoins.c: 1184 - 1291

## Overview
Determines whether an inner relation provably contains at most one tuple matching any tuple from the outer relation, using join clauses and caching results for performance optimization.

## Definition
```c
bool innerrel_is_unique(PlannerInfo *root,
                       Relids joinrelids,
                       Relids outerrelids, 
                       RelOptInfo *innerrel,
                       JoinType jointype,
                       List *restrictlist,
                       bool force_cache)
```

## Detailed Description
This function serves as a frontend to `is_innerrel_unique_for()` with sophisticated caching capabilities to avoid redundant uniqueness proofs during query planning. It determines whether the inner relation can be proven to contain at most one matching tuple for any given outer relation tuple, based on the join restriction clauses.

The function implements several optimizations:
1. **Quick elimination**: Returns false immediately if no join clauses exist or if the relation doesn't support distinctness analysis
2. **Positive caching**: Caches successful uniqueness proofs for reuse with relation subsets 
3. **Negative caching**: Optionally caches failed proofs to avoid redundant analysis in GEQO mode or with join search plugins
4. **Asymmetric design**: Requires actual RelOptInfo for inner relation but only Relids for outer relation to support early planning phases

The function distinguishes between "joinquals" and "otherquals" for outer joins, ignoring pushed-down quals that would become "otherquals" at execution time. This affects the proof validity depending on whether the join is classified as outer by IS_OUTER_JOIN().

## Parameters / Member Variables
- `root`: PlannerInfo containing global planning state and memory contexts
- `joinrelids`: Combined Relids of both outer and inner relations for the join
- `outerrelids`: Relids identifying the outer relation(s) in the join
- `innerrel`: RelOptInfo structure for the inner relation being tested
- `jointype`: Type of join operation (affects which quals are considered)
- `restrictlist`: List of join restriction clauses to analyze for uniqueness proof
- `force_cache`: Boolean flag to override heuristics and force negative result caching

## Dependencies
- Functions called/Symbols referenced:
  - rel_supports_distinctness
  - is_innerrel_unique_for
  - bms_is_subset
  - bms_copy  
  - lappend
  - MemoryContextSwitchTo
- Data structures used:
  - PlannerInfo
  - RelOptInfo
  - JoinType
  - Relids
- Called from:
  - add_paths_to_joinrel (src/backend/optimizer/path/joinpath.c:185, 194)
  - reduce_unique_semijoins (src/backend/optimizer/plan/analyzejoins.c:783)

## Notes and Other Information
- Results are cached in `innerrel->unique_for_rels` (positive) and `innerrel->non_unique_for_rels` (negative)
- Cached results use planner_cxt memory context to persist across GEQO attempts
- Positive cache entries work for any subset of the proven outer relations  
- Negative cache entries prevent testing any superset of the failed outer relations
- Caching negative results is normally disabled in standard planning but enabled in GEQO mode
- The `force_cache` parameter allows overriding caching heuristics for special cases like `reduce_unique_semijoins`
- Critical for join elimination optimizations and semi-join reduction strategies