# ExecReScanSetParamPlan

## Location
[src/backend/executor/nodeSubplan.c:1291-1328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubplan.c#L1291-L1328)

## Overview
ExecReScanSetParamPlan marks an initplan as needing recalculation by setting its output parameters as dirty and updating the parent node's change parameter bitmap to trigger rescanning of dependent nodes.

## Definition
```c
void ExecReScanSetParamPlan(SubPlanState *node, PlanState *parent)
```

## Detailed Description
ExecReScanSetParamPlan is responsible for marking an initplan's output parameters as needing recalculation when a rescan is required. The function performs several validation checks to ensure the subplan is properly configured as an initplan, then marks the output parameters as dirty by setting their execPlan field back to the SubPlanState node (except for CTE subplans which have special handling).

The function does not actually perform the rescan - that will happen inside ExecSetParamPlan when the parameters are next accessed and found to need recalculation. Instead, it prepares the parameter state for lazy re-evaluation and updates the parent node's chgParam bitmap to ensure dependent plan nodes are informed they need to rescan.

Special handling is provided for CTE (Common Table Expression) subplans, which are executed via nodeCtescan.c rather than through parameter recalculation. For CTE subplans, the output parameter is not marked as dirty, but the chgParam bit is still set to notify dependent nodes.

## Parameters / Member Variables
- `node`: The SubPlanState representing the initplan that needs to be marked for recalculation
- `parent`: The parent PlanState node that will have its chgParam bitmap updated to reflect the changed parameters

## Dependencies
- Functions called/Symbols referenced:
  - [SubPlanState](../S/SubPlanState.md)
  - SubPlan
  - bms_is_empty
  - lfirst_int
  - [ParamExecData](../P/ParamExecData.md)
  - CTE_SUBLINK
  - [bms_add_member](../b/bms_add_member.md)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md)

## Notes and Other Information
- The function performs validation checks to ensure the subplan is properly configured as an initplan (no parParam, has setParam, has extParam)
- CTE subplans receive special treatment - they don't get their execPlan marked as dirty but still update chgParam
- The actual rescan is deferred until the parameter values are needed (lazy evaluation)
- Direct correlated subqueries are not supported as initplans and will cause an error
- The function updates the parent's chgParam bitmap to ensure dependent plan nodes know to rescan when parameters change