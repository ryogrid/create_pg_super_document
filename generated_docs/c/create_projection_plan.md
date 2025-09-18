# create_projection_plan

## Location
src/backend/optimizer/plan/createplan.c: 2019 - 2120

## Overview
Creates a projection plan that computes a specific target list of expressions, optionally adding a Result node if the subplan cannot directly produce the required output.

## Definition


## Detailed Description
The  function implements PostgreSQL's projection step, which computes a specific set of expressions (target list) from input tuples. The function intelligently determines whether a separate Result node is needed or if the projection can be pushed down to the subplan.

The function follows a three-way decision process:

1. **Physical tlist usage**: If  returns true, the caller doesn't require an exact target list, so projection may not be needed. The subplan's natural output is used, with optional labeling if CP_LABEL_TLIST is specified.

2. **Projection-capable subplan**: If the subplan can perform projection itself (e.g., Scan nodes), the projection is pushed down by using CP_IGNORE_TLIST flag and building the required target list.

3. **Result node required**: If the subplan cannot perform the projection or produces a different target list than required, a Result node is added to perform the projection step.

The function includes cost estimation logic and handles the case where the actual implementation decision differs from what was estimated during path creation.

## Parameters / Member Variables
- : PlannerInfo containing planner state and context
- : ProjectionPath specifying the projection requirements and subpath
- : Control flags including CP_LABEL_TLIST for sortgroupref labeling

## Dependencies
- Functions called/Symbols referenced:
  - [use_physical_tlist](../u/use_physical_tlist.md)
  - [create_plan_recurse](create_plan_recurse.md) (with various flags)
  - [apply_pathtarget_labeling_to_tlist](../a/apply_pathtarget_labeling_to_tlist.md)
  - is_projection_capable_path
  - is_projection_capable_plan
  - [build_path_tlist](../b/build_path_tlist.md)
  - [tlist_same_exprs](../t/tlist_same_exprs.md)
  - [make_result](../m/make_result.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- The function rechecks projection requirements even if the path creation phase set dummypp, because some createplan.c routines modify target lists after plan creation
- When a Result node is not needed, the function directly assigns the target list to the subplan and updates cost estimates
- The parallel_aware flag of the subplan is preserved even when other plan properties are updated
- Cost estimates may be slightly wrong if the Result node decision differs from path creation, but the function uses the estimates from the actual implementation
- Sortgroupref labeling is applied when CP_LABEL_TLIST flag is set, ensuring proper GROUP BY and ORDER BY reference handling