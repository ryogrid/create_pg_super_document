# create_minmaxagg_path

## Location
[src/backend/optimizer/util/pathnode.c:3397-3484](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3397-L3484)

## Overview
Creates a pathnode that represents computation of MIN/MAX aggregates using index scans to efficiently find minimum and maximum values without scanning the entire table.

## Definition


## Detailed Description
This function creates a MinMaxAggPath node that represents an optimized approach to computing MIN/MAX aggregates. Instead of performing a full table scan and aggregation, this path uses index scans to directly find the minimum and maximum values. The resulting plan will be a Result node that executes initplans for each MIN/MAX aggregate. The function calculates costs by summing up the pathcosts of all initplans and adds target evaluation costs. It also performs parallel safety checks on all components including initplans, target expressions, and qualification conditions.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and configuration information
- : RelOptInfo representing the parent relation associated with the result
- : PathTarget structure defining the target list to be computed
- : List of MinMaxAggInfo structures containing information about MIN/MAX aggregates
- : List containing HAVING clause qualifications, if any

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [cost_qual_eval](cost_qual_eval.md)
  - [is_parallel_safe](../i/is_parallel_safe.md)
  - lfirst
  - cpu_tuple_cost
- Called from (representative examples):
  - [preprocess_minmax_aggregates](../p/preprocess_minmax_aggregates.md) (src/backend/optimizer/plan/planagg.c:220)

## Notes and Other Information
- The topmost generated Plan node will be a Result node, not an Agg node
- Always produces exactly one output row regardless of input size
- Assumes no parameterization (above any joins) for simplification
- Parallel safety depends on all initplans, target expressions, and quals being parallel-safe
- The Result node itself is not parallelizable, but parallel safety information is useful for outer queries in subquery scenarios
- Cost calculation includes initplan costs, target evaluation costs, cpu_tuple_cost, and optional qualification costs
- Rowcount estimate is always 1, regardless of qualification selectivity