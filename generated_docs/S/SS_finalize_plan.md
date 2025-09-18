# SS_finalize_plan

## Location
[src/backend/optimizer/plan/subselect.c:2254-2291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L2254-L2291)

## Overview
Performs final parameter processing for a completed Plan tree by recursively computing the extParam and allParam sets for every Plan node.

## Definition
```c
void SS_finalize_plan(PlannerInfo *root, Plan *plan)
```

## Detailed Description
SS_finalize_plan is the top-level function responsible for finalizing parameter information throughout an entire plan tree. This function serves as the entry point for the parameter finalization process, which computes parameter dependency information that is crucial for proper plan execution.

The function recursively processes the entire plan tree to compute two important parameter sets for each plan node:
- extParam: Parameters that are supplied from outside the current plan node
- allParam: All parameters that the plan node and its subtree depend on

Additionally, it handles parameter processing for RangeTblFunction.funcparams. The function assumes that any referenced initplans or subplans have already been processed through SS_finalize_plan.

The actual recursive processing is delegated to the finalize_plan function, which does the heavy lifting of parameter analysis.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the planning context, including outer_params that represent parameters from outer query levels
- `plan`: The root Plan node of the plan tree to be finalized

## Dependencies
- Functions called/Symbols referenced:
  - [finalize_plan](../f/finalize_plan.md)
- Called from (representative examples):
  - [standard_planner](../s/standard_planner.md) (src/backend/optimizer/plan/planner.c:515, 517)

## Notes and Other Information
- This function is part of PostgreSQL's subselect processing subsystem
- Must be called after all initplans and subplans have been finalized
- The function serves as a thin wrapper around finalize_plan, providing the initial parameters for the recursive traversal
- Parameter finalization is essential for proper handling of correlated subqueries and nested plan execution
- Declared in src/include/optimizer/subselect.h