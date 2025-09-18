# create_groupingsets_path

## Location
src/backend/optimizer/util/pathnode.c: 3237 - 3396

## Overview
Creates a pathnode that represents performing GROUPING SETS aggregation with one or more grouping sets, where the input path's result must be sorted to match the last entry in rollup_groupclauses.

## Definition


## Detailed Description
This function creates a GroupingSetsPath node that represents sorted grouping with one or more grouping sets. The function handles different aggregation strategies (AGG_SORTED, AGG_PLAIN, AGG_HASHED, AGG_MIXED) and can simplify them when appropriate. It calculates the total cost by iterating through each rollup operation, considering whether each rollup is hashed or sorted, and accounting for sorting costs when necessary. The output will be in sorted order by group_pathkeys only if there is a single rollup operation on a non-empty list of grouping expressions.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and configuration
- : RelOptInfo representing the parent relation associated with the result
- : Path representing the source of input data
- : List containing HAVING clause qualifications, if any
- : AggStrategy enum specifying the aggregation strategy to use
- : List of RollupData nodes defining the rollup operations
- : AggClauseCosts structure containing cost information about aggregate functions

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - cost_agg
  - cost_sort
  - list_length
  - linitial
  - lfirst
- Called from (representative examples):
  - consider_groupingsets_paths (src/backend/optimizer/plan/planner.c:4377)
  - consider_groupingsets_paths (src/backend/optimizer/plan/planner.c:4535)
  - consider_groupingsets_paths (src/backend/optimizer/plan/planner.c:4550)

## Notes and Other Information
- The function simplifies aggregation strategies when possible: AGG_SORTED to AGG_PLAIN for single rollups with no grouping clause, and AGG_MIXED to AGG_HASHED for single rollups
- In AGG_SORTED/AGG_PLAIN mode, the first rollup uses already-sorted input while subsequent ones perform their own sort
- In AGG_HASHED mode, there is one rollup per grouping set
- In AGG_MIXED mode, initial rollups are hashed, the first non-hashed rollup uses sorted input, and following ones sort themselves
- The pathnode's pathkeys are set to root->group_pathkeys only for AGG_SORTED strategy with a single rollup