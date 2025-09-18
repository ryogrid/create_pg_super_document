# create_agg_path

## Location
[src/backend/optimizer/util/pathnode.c:3155-3236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3155-L3236)

## Overview
Creates a pathnode that represents performing aggregation and/or grouping operations with various implementation strategies and aggregate splitting modes.

## Definition
```c
AggPath *create_agg_path(PlannerInfo *root,
                        RelOptInfo *rel,
                        Path *subpath,
                        PathTarget *target,
                        AggStrategy aggstrategy,
                        AggSplit aggsplit,
                        List *groupClause,
                        List *qual,
                        const AggClauseCosts *aggcosts,
                        double numGroups)
```

## Detailed Description
This function creates an AggPath node that represents aggregation operations with various strategies including hashed, sorted, and mixed approaches. The function handles different aggregation scenarios from simple aggregate functions without grouping to complex GROUP BY operations with HAVING clauses. It supports parallel aggregation through the aggsplit parameter and can preserve input ordering for sorted aggregation strategies.

The function sets up pathkey handling based on the aggregation strategy - preserving order for sorted aggregation while stripping pathkeys specific to aggregate function internals, and removing ordering for hashed aggregation. Cost calculation uses `cost_agg` and includes target list evaluation costs.

## Parameters / Member Variables
- `root`: PlannerInfo containing planning context, groupby pathkey information, and optimizer settings
- `rel`: RelOptInfo representing the parent relation associated with the result
- `subpath`: Path representing the source of input data for aggregation
- `target`: PathTarget specifying the expressions to be computed in the output
- `aggstrategy`: AggStrategy enum specifying the implementation approach (sorted, hashed, mixed, plain)
- `aggsplit`: AggSplit enum indicating aggregate splitting mode for parallel processing
- `groupClause`: List of SortGroupClause structures representing the grouping specification
- `qual`: List of HAVING qualification expressions to filter groups, if any
- `aggcosts`: AggClauseCosts structure containing cost information about aggregate functions
- `numGroups`: Estimated number of output groups (1 for non-grouping aggregation)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates AggPath node)
  - [cost_agg](cost_agg.md) (calculates aggregation operation costs)
  - list_length (counts grouping columns and pathkeys)
  - [list_copy_head](../l/list_copy_head.md) (preserves subset of input pathkeys for sorted aggregation)
  - AggPath (return type structure)
  - [PathTarget](../P/PathTarget.md) (target list specification)
- Called from (representative examples):
  - [create_partial_distinct_paths](create_partial_distinct_paths.md)
  - [create_final_distinct_paths](create_final_distinct_paths.md)
  - [add_paths_to_grouping_rel](../a/add_paths_to_grouping_rel.md)
  - [create_partial_grouping_paths](create_partial_grouping_paths.md)
  - [generate_union_paths](../g/generate_union_paths.md)

## Notes and Other Information
- Supports multiple aggregation strategies: AGG_SORTED (preserves/manages input order), AGG_HASHED (no output ordering), AGG_MIXED, and AGG_PLAIN
- For sorted aggregation, carefully manages pathkeys to preserve grouping order while stripping aggregate function internal ordering
- Includes transition space tracking for aggregate function state management
- Assumes operation above joins (no parameterization) and inherits parallel safety from subpath
- HAVING qualifications are applied during aggregation to filter groups
- Target list evaluation costs are added separately from the core aggregation costs
- The aggsplit parameter enables parallel aggregation by splitting aggregate computation across workers
- Handles both simple aggregation (COUNT(*), SUM()) and complex GROUP BY operations