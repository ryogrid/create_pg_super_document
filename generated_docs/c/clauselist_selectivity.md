# clauselist_selectivity

## Location
[src/backend/optimizer/path/clausesel.c:100-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/clausesel.c#L100-L116)

## Overview
Computes the selectivity of an implicitly-ANDed list of boolean expression clauses, serving as the main entry point for estimating the selectivity of combined WHERE clause conditions in PostgreSQL's query optimizer.

## Definition
```c
Selectivity clauselist_selectivity(PlannerInfo *root,
                                   List *clauses,
                                   int varRelid,
                                   JoinType jointype,
                                   SpecialJoinInfo *sjinfo)
```

## Detailed Description
This function is a wrapper around `clauselist_selectivity_ext` that computes the selectivity of a list of clauses combined with AND logic. The function handles empty clause lists by returning 1.0 (meaning no filtering). It supports both RestrictInfo structures and bare expression clauses, with RestrictInfo being preferred for caching purposes.

The function delegates the actual computation to `clauselist_selectivity_ext` with extended statistics enabled (true parameter), which allows for more sophisticated selectivity estimation using cross-column dependencies, range queries optimization, and statistical correlation analysis.

Key features include:
- Handles implicitly-ANDed boolean expression clauses
- Supports both RestrictInfo and bare expression clauses
- Returns 1.0 for empty clause lists (no filtering)
- Enables extended statistics for better accuracy
- Used throughout the optimizer for cost estimation

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and statistics
- `clauses`: List of boolean expression clauses to evaluate (can be empty)
- `varRelid`: Variable relation ID for context-specific estimation
- `jointype`: Type of join operation (affects selectivity calculation)
- `sjinfo`: Special join information for complex join scenarios

## Dependencies
- Functions called/Symbols referenced:
  - [clauselist_selectivity_ext](clauselist_selectivity_ext.md)
  - JoinType
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
- Called from (representative examples):
  - [cost_tidrangescan](cost_tidrangescan.md)
  - [cost_subqueryscan](cost_subqueryscan.md)
  - [cost_agg](cost_agg.md)
  - [cost_group](cost_group.md)
  - [compute_semi_anti_join_factors](compute_semi_anti_join_factors.md)
  - [set_baserel_size_estimates](../s/set_baserel_size_estimates.md)
  - [get_parameterized_baserel_size](../g/get_parameterized_baserel_size.md)
  - [calc_joinrel_size_estimate](calc_joinrel_size_estimate.md)
  - [genericcostestimate](../g/genericcostestimate.md)
  - [btcostestimate](../b/btcostestimate.md)
  - [gincostestimate](../g/gincostestimate.md)
  - [brincostestimate](../b/brincostestimate.md)

## Notes and Other Information
This function serves as the primary interface for selectivity estimation in PostgreSQL's cost-based optimizer. It's heavily used throughout the costing system for estimating row counts and query execution costs. The function is designed to be a simple wrapper that enables extended statistics by default, delegating complex logic to `clauselist_selectivity_ext`. The selectivity value returned (0.0 to 1.0) represents the fraction of rows expected to pass the combined clause conditions.

## Simplified Source

```c
Selectivity
clauselist_selectivity(PlannerInfo *root,
                       List *clauses,
                       int varRelid,
                       JoinType jointype,
                       SpecialJoinInfo *sjinfo)
{
    // Delegate to extended function with extended statistics enabled
    return clauselist_selectivity_ext(root, clauses, varRelid,
                                     jointype, sjinfo, true);
}
```