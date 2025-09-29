# ExecReScanNestLoop

## Location
[src/backend/executor/nodeNestloop.c:381-400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeNestloop.c#L381-L400)

## Overview
ExecReScanNestLoop resets a nested loop join node to restart execution from the beginning, handling outer plan rescanning while maintaining proper inner plan scan coordination.

## Definition
```c
void ExecReScanNestLoop(NestLoopState *node)
```

## Detailed Description
ExecReScanNestLoop is responsible for resetting a nested loop join to restart execution from the beginning. This function is typically called when the nested loop join needs to be re-executed, such as when it's part of a subquery that needs to be re-evaluated with different parameter values.

The function implements a careful approach to rescanning that considers the execution state of child nodes. For the outer plan, it only calls ExecReScan if the outer plan's chgParam (changed parameters) is NULL. If chgParam is not NULL, it means the outer plan will automatically be re-scanned when ExecProcNode is next called on it, so explicit rescanning is unnecessary.

Critically, the function does NOT rescan the inner plan from this level. This is because the inner plan is rescanned for each new outer tuple during normal nested loop execution. Rescanning the inner plan here could cause problems, particularly with inner index scans that use outer variables as run-time keys, as noted in the detailed comment.

The function resets the nested loop's state flags to indicate that a new outer tuple is needed and that no outer tuple has been matched yet, effectively returning the join to its initial execution state.

## Parameters / Member Variables
- `node`: The NestLoopState containing the execution state to be reset

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState: Accesses the outer child plan state
  - [ExecReScan](ExecReScan.md): Rescans the outer plan if necessary
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md): As part of general plan node rescanning operations

## Notes and Other Information
- Does not rescan the inner plan to avoid conflicts with parameterized inner scans
- Only rescans outer plan when chgParam is NULL to avoid redundant operations
- Resets nl_NeedNewOuter to true and nl_MatchedOuter to false for clean restart
- Critical for correct execution of nested loops within subqueries and correlated queries
- Part of the standard rescan protocol for plan nodes that can be re-executed
- Handles the complex interaction between parameter changes and scan state management

## Simplified Source

```c
void ExecReScanNestLoop(NestLoopState *node) {
    PlanState *outerPlan = outerPlanState(node);

    // Rescan outer plan only if no parameters changed
    if (outerPlan->chgParam == NULL)
        ExecReScan(outerPlan);

    // Reset nested loop state flags
    // Note: Inner plan is NOT rescanned here - it's rescanned
    // for each outer tuple to handle parameterized scans properly
    node->nl_NeedNewOuter = true;
    node->nl_MatchedOuter = false;
}
```