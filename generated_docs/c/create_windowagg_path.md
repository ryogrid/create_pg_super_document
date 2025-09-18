# create_windowagg_path

## Location
[src/backend/optimizer/util/pathnode.c:3485-3554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3485-L3554)

## Overview
Creates a pathnode that represents computation of window functions, where the input must be sorted according to the WindowClause's PARTITION keys plus ORDER BY keys.

## Definition


## Detailed Description
This function creates a WindowAggPath node that represents the execution of window functions. Window functions are computed over a set of rows related to the current row within a partition, and they require the input to be properly sorted by partition and order keys. The function preserves the input sort order and can handle both top-level and intermediate WindowAgg operations. For costing purposes, it assumes no redundant partitioning or ordering columns and delegates to cost_windowagg for detailed cost calculation. The path can include run conditions for short-circuiting execution and qualification conditions for top-level windows.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and configuration
- : RelOptInfo representing the parent relation associated with the result
- : Path representing the source of input data (must be properly sorted)
- : PathTarget structure defining the target list to be computed
- : List of WindowFunc structures representing window functions to compute
- : List of OpExprs used to short-circuit WindowAgg execution when possible
- : WindowClause structure common to all the WindowFuncs being processed
- : List of qualification conditions from lower-level WindowAggPaths (must be NIL unless topwindow is true)
- : Boolean flag indicating if this is the top-level WindowAgg (true) or intermediate (false)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [cost_windowagg](cost_windowagg.md)
  - Assert
- Called from (representative examples):
  - [create_one_window_path](create_one_window_path.md) (src/backend/optimizer/plan/planner.c:4809)

## Notes and Other Information
- The input data must be sorted according to the WindowClause's PARTITION keys plus ORDER BY keys
- WindowAgg preserves the input sort order in its output
- For now, assumes no parameterization (above any joins) for simplification
- Parallel safety depends on the relation's consider_parallel flag and subpath's parallel safety
- The qual parameter can only be set when topwindow is true, enforced by an assertion
- Cost calculation assumes no redundant partitioning or ordering columns for simplicity
- The function adds target evaluation costs on top of the base window aggregation costs
- Run conditions allow for potential short-circuiting of WindowAgg execution to improve performance