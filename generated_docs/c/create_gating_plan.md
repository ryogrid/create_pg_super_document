# create_gating_plan

## Location
[src/backend/optimizer/plan/createplan.c:1023-1081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L1023-L1081)

## Overview
Creates a gating Result node to handle pseudoconstant qualification clauses by adding it atop an already-built execution plan.

## Definition
```c
static Plan *create_gating_plan(PlannerInfo *root, Path *path, Plan *plan, List *gating_quals)
```

## Detailed Description
The `create_gating_plan` function creates a Result node that acts as a "gate" in the execution plan to evaluate pseudoconstant qualifiers early. This optimization allows the query executor to potentially short-circuit expensive operations when the gating conditions fail. The function wraps an existing plan with a Result node that contains the gating qualifiers, effectively creating a conditional execution layer. The function is careful to avoid creating redundant Result nodes by checking if the input plan is already a trivial Result node.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information
- `path`: Path structure representing the access path being converted to a plan
- `plan`: The existing Plan node that will be wrapped with the gating Result node
- `gating_quals`: List of pseudoconstant qualification clauses to be evaluated by the gating node

## Dependencies
- Functions called/Symbols referenced:
  - [build_path_tlist](../b/build_path_tlist.md)
  - [make_result](../m/make_result.md)
  - [copy_plan_costsize](copy_plan_costsize.md)
  - [Result](../R/Result.md) (type/struct)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)
  - [create_join_plan](create_join_plan.md)

## Notes and Other Information
- The function avoids stacking Result nodes unnecessarily by checking if the input plan is already a trivial Result node
- Cost and size estimates remain unchanged when adding gating, assuming the gating qual will succeed (conservative estimate)
- The parallel safety flag is inherited from the Path rather than the Plan to account for potentially unsafe gating quals
- Gating quals are typically pseudoconstant conditions that can be evaluated once and used to control execution flow
- The function always returns the path's requested target list, ensuring compatibility with parent node expectations