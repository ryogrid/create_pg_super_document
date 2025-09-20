# create_limit_path

## Location
[src/backend/optimizer/util/pathnode.c:3826-3880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3826-L3880)

## Overview
Creates a pathnode that represents performing LIMIT/OFFSET operations on query results, enabling efficient handling of result set truncation and pagination in the query planner.

## Definition

```c
LimitPath *
create_limit_path(PlannerInfo *root, RelOptInfo *rel,
				  Path *subpath,
				  Node *limitOffset, Node *limitCount,
				  LimitOption limitOption,
				  int64 offset_est, int64 count_est)
```
## Detailed Description
This function creates a LimitPath node that represents LIMIT and OFFSET operations in PostgreSQL's query planning system. It wraps an existing subpath and adds the necessary metadata for limiting result sets. The function preserves important properties like sort order (pathkeys) from the subpath since LIMIT/OFFSET operations don't change the ordering of results.

The function supports parallel execution when the underlying subpath is parallel-safe and the relation allows parallel processing. Cost and row count adjustments are performed using the adjust_limit_rows_costs function, which takes into account the estimated OFFSET and LIMIT values to provide more accurate cost estimates.

The implementation handles cases where OFFSET or LIMIT expressions might not be present (represented as NULL) or where their values cannot be estimated at planning time (represented as -1 in the estimates).

## Parameters / Member Variables
- : PlannerInfo structure containing global planning information
- : RelOptInfo representing the parent relation associated with the result
- : Path representing the source of data to be limited
- : Actual OFFSET expression node, or NULL if not present
- : Actual LIMIT expression node, or NULL if not present
- : LimitOption specifying additional limit behavior options
- : Estimated value of OFFSET (0 = not present, -1 = cannot estimate)
- : Estimated value of LIMIT (0 = not present, -1 = cannot estimate)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create LimitPath node)
  - [adjust_limit_rows_costs](../a/adjust_limit_rows_costs.md) (to adjust costs and row estimates based on limit/offset)
  - [LimitOption](../L/LimitOption.md) (limit behavior option type)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:1815)
  - [create_partial_distinct_paths](create_partial_distinct_paths.md) (src/backend/optimizer/plan/planner.c:5020)
  - [create_final_distinct_paths](create_final_distinct_paths.md) (src/backend/optimizer/plan/planner.c:5235)

## Notes and Other Information
- Preserves pathkeys (sort order) from the subpath since LIMIT/OFFSET don't change ordering
- Supports parallel execution when conditions are met (subpath is parallel-safe and relation considers parallel)
- Uses the same pathtarget as the subpath since LIMIT doesn't project new columns
- Cost estimation considers both startup and total costs, adjusted for the limiting behavior
- Estimate values of 0 indicate the clause is not present, -1 indicates it's present but value unknown
- The function assumes operations are above joins, so no parameterization is needed
- Critical for implementing efficient pagination and result set truncation in queries