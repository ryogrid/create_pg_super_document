# ExecSubPlan

## Location
[src/backend/executor/nodeSubplan.c:62-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubplan.c#L62-L100)

## Overview
ExecSubPlan is the main entry point for executing a regular SubPlan in PostgreSQL's executor, handling the evaluation of subqueries by selecting the appropriate execution strategy (hash table or scan-based).

## Definition
```c
Datum ExecSubPlan(SubPlanState *node, ExprContext *econtext, bool *isNull)
```

## Detailed Description
ExecSubPlan serves as the primary execution function for SubPlan nodes in PostgreSQL's query executor. It performs essential setup and validation before delegating the actual subquery execution to specialized functions. The function enforces forward-scan direction during execution and restores the original scan direction afterward. It validates that CTE sublinks are not executed through this path and ensures proper parameter handling for different sublink types.

The function implements a strategy pattern by choosing between hash-based execution (ExecHashSubPlan) for subplans that use hash tables, or scan-based execution (ExecScanSubPlan) for traditional sequential processing.

## Parameters / Member Variables
- `node`: SubPlanState containing the execution state and configuration for the subplan
- `econtext`: ExprContext providing the evaluation context and parameter values
- `isNull`: Pointer to boolean flag indicating whether the result should be considered NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ExecHashSubPlan](ExecHashSubPlan.md) (for hash table-based subplan execution)
  - [ExecScanSubPlan](ExecScanSubPlan.md) (for scan-based subplan execution)
  - CHECK_FOR_INTERRUPTS (for query cancellation handling)
  - elog (for error reporting)
- Called from (representative examples):
  - [ExecEvalSubPlan](ExecEvalSubPlan.md) (in execExprInterp.c:4760)

## Notes and Other Information
- Forces forward-scan direction during subplan execution to ensure consistent behavior
- Explicitly prevents execution of CTE sublinks, which have their own execution path
- Validates parameter handling constraints for different sublink types
- Returns a Datum value representing the subplan's execution result
- The function is declared in nodeSubplan.h and is part of the executor's subplan handling infrastructure

## Simplified Source

```c
Datum ExecSubPlan(SubPlanState *node, ExprContext *econtext, bool *isNull)
{
    SubPlan *subplan = node->subplan;
    EState *estate = node->planstate->state;
    ScanDirection dir = estate->es_direction;
    Datum retval;

    CHECK_FOR_INTERRUPTS();

    // Initialize result as non-null
    *isNull = false;

    // Validate subplan type - CTE sublinks use different execution path
    if (subplan->subLinkType == CTE_SUBLINK)
        elog(ERROR, "CTE subplans should not be executed via ExecSubPlan");

    // Validate parameter handling for sublink types
    if (subplan->setParam != NIL && subplan->subLinkType != MULTIEXPR_SUBLINK)
        elog(ERROR, "cannot set parent params from subquery");

    // Force forward scan direction for consistent execution
    estate->es_direction = ForwardScanDirection;

    // Choose execution strategy based on subplan configuration
    if (subplan->useHashTable)
        retval = ExecHashSubPlan(node, econtext, isNull);
    else
        retval = ExecScanSubPlan(node, econtext, isNull);

    // Restore original scan direction
    estate->es_direction = dir;

    return retval;
}
```