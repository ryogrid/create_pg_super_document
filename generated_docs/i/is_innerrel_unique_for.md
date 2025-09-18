# is_innerrel_unique_for

## Location
[src/backend/optimizer/plan/analyzejoins.c:1292-1340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/analyzejoins.c#L1292-L1340)

## Overview
The core implementation function that performs the actual uniqueness proof for an inner relation against an outer relation by filtering and analyzing join restriction clauses.

## Definition
```c
static bool is_innerrel_unique_for(PlannerInfo *root,
                                  Relids joinrelids,
                                  Relids outerrelids,
                                  RelOptInfo *innerrel,
                                  JoinType jointype,
                                  List *restrictlist)
```

## Detailed Description
This function performs the actual work of proving whether an inner relation is unique for a given outer relation. It carefully filters the join restriction clauses to identify those that can contribute to a uniqueness proof, then delegates the final analysis to `rel_is_distinct_for()`.

The function applies several filtering criteria to select suitable clauses:
1. **Outer join handling**: Excludes pushed-down clauses for outer joins since they become "otherquals" rather than "joinquals" at execution
2. **Mergejoinability**: Only considers clauses that are mergejoinable, which behave like equality for some btree opclass and exclude volatile functions
3. **Clause structure**: Ensures clauses have the proper "outer op inner" or "inner op outer" form using `clause_sides_match_join()`

The mergejoinability requirement is crucial because:
- Mergejoinable operators behave like equality for btree opclasses
- They exclude volatile functions that could break uniqueness assumptions
- They provide the necessary semantic guarantees for uniqueness proofs

After filtering suitable clauses, the function passes them to `rel_is_distinct_for()` which performs the final distinctness analysis using indexes, constraints, and other relation properties.

## Parameters / Member Variables
- `root`: PlannerInfo containing global planning state and context
- `joinrelids`: Combined Relids of both outer and inner relations in the join  
- `outerrelids`: Relids identifying the outer relation(s) for the join
- `innerrel`: RelOptInfo structure for the inner relation being tested for uniqueness
- `jointype`: Type of join operation (affects handling of pushed-down clauses)
- `restrictlist`: List of RestrictInfo join clauses to analyze

## Dependencies
- Functions called/Symbols referenced:
  - IS_OUTER_JOIN (macro to test if join type is outer)
  - RINFO_IS_PUSHED_DOWN (macro to test if clause is pushed down)
  - [clause_sides_match_join](../c/clause_sides_match_join.md) (validates clause structure for join relations)
  - [rel_is_distinct_for](../r/rel_is_distinct_for.md) (performs final distinctness analysis)
  - lappend (appends to PostgreSQL list)
- Data structures used:
  - [RestrictInfo](../R/RestrictInfo.md)
  - [PlannerInfo](../P/PlannerInfo.md)  
  - RelOptInfo
  - JoinType
  - Relids
- Called from:
  - [innerrel_is_unique](innerrel_is_unique.md) (src/backend/optimizer/plan/analyzejoins.c:1234)

## Notes and Other Information
- Declared as static, so only visible within analyzejoins.c
- Serves as the filtering layer between `innerrel_is_unique()` (caching frontend) and `rel_is_distinct_for()` (analysis backend)
- The mergejoinability test is critical for correctness - it ensures operators behave like equality and excludes problematic volatile functions
- Pushed-down clause handling is essential for outer joins where some clauses change from joinquals to otherquals
- The filtered clause list passed to `rel_is_distinct_for()` contains only clauses that can meaningfully contribute to uniqueness proofs
- Part of PostgreSQL's sophisticated join optimization infrastructure that enables advanced optimizations like join elimination