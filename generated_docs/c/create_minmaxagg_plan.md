# create_minmaxagg_plan

## Location
[src/backend/optimizer/plan/createplan.c:2551-2616](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2551-L2616)

## Overview
Creates a Result plan for MinMaxAggPath operations that optimize MIN/MAX aggregates by converting them into InitPlan subqueries for more efficient execution.

## Definition
```c
static Result *create_minmaxagg_plan(PlannerInfo *root, MinMaxAggPath *best_path)
```

## Detailed Description
The `create_minmaxagg_plan` function implements an optimization for queries containing MIN/MAX aggregates by converting them into InitPlan subqueries rather than full aggregation operations. For each aggregate in the mmaggregates list, it creates a subplan, adds a LIMIT node to ensure only one row is returned, and converts the entire subplan into an InitPlan that can be executed once and referenced by parameter. The final plan is a simple Result node that uses the InitPlan parameters to produce the final output. This optimization is particularly effective when MIN/MAX operations can be satisfied by index scans that naturally return results in the desired order.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: MinMaxAggPath structure containing the list of MinMaxAggInfo structures for each aggregate to be optimized

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan](create_plan.md)
  - [make_limit](../m/make_limit.md)
  - [SS_make_initplan_from_plan](../S/SS_make_initplan_from_plan.md)
  - [build_path_tlist](../b/build_path_tlist.md)
  - [make_result](../m/make_result.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- The function is static, used only within createplan.c
- Each aggregate gets its own InitPlan subquery with a LIMIT node to ensure only one row is returned
- Uses create_plan (not create_plan_recurse) since it enters a different planner context (subroot)
- Cost information is carefully maintained through the LIMIT node to preserve optimizer estimates
- Stores the mmaggregates list in root->minmax_aggs for later use by setrefs.c to replace Agg node references with InitPlan parameters
- The optimization is most effective with ordered index scans where MIN/MAX can be satisfied by reading the first/last row
- Results in significant performance improvements for queries like "SELECT MIN(id) FROM table" when an index exists on the column