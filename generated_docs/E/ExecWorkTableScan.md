# ExecWorkTableScan

## Location
[src/backend/executor/nodeWorktablescan.c:81-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWorktablescan.c#L81-L129)

## Overview
ExecWorkTableScan is the main execution function for worktable scan nodes, orchestrating the sequential scanning of temporary worktables used in recursive queries and returning qualifying tuples.

## Definition
static TupleTableSlot *ExecWorkTableScan(PlanState *pstate)

## Detailed Description
ExecWorkTableScan implements the primary execution logic for WorkTableScan plan nodes in PostgreSQL's executor. It handles the initialization and execution of scans over temporary worktables used during recursive query processing. The function performs lazy initialization on first call, locating the ancestor RecursiveUnion state through a parameter slot mechanism, and then configures the scan's tuple type and projection information. Once initialized, it delegates the actual scanning to the generic ExecScan framework, providing WorkTableScan-specific access methods for tuple retrieval and rechecking. The function handles the complex initialization timing issues where the WorkTableScan node may be initialized before its parent RecursiveUnion node.

## Parameters / Member Variables
- `pstate`: PlanState pointer that gets cast to WorkTableScanState, containing the execution state for the worktable scan

## Dependencies
- Functions called/Symbols referenced:
  - castNode (type casting with assertion)
  - [DatumGetPointer](../D/DatumGetPointer.md) (extracts pointer from Datum)
  - [ExecAssignScanType](ExecAssignScanType.md) (assigns scan tuple descriptor)
  - [ExecGetResultType](ExecGetResultType.md) (gets result tuple type from plan state)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md) (initializes projection information)
  - [ExecScan](ExecScan.md) (generic scan execution framework)
  - [WorkTableScanNext](../W/WorkTableScanNext.md) (access method for next tuple)
  - [WorkTableScanRecheck](../W/WorkTableScanRecheck.md) (recheck method for EvalPlanQual)
- Types used:
  - [WorkTableScanState](../W/WorkTableScanState.md) (scan execution state)
  - [WorkTableScan](../W/WorkTableScan.md) (plan node structure)
  - [EState](EState.md) (executor state)
  - [ParamExecData](../P/ParamExecData.md) (executor parameter data)
  - [RecursiveUnionState](../R/RecursiveUnionState.md) (recursive union execution state)
- Called from:
  - [ExecInitWorkTableScan](ExecInitWorkTableScan.md) (during plan node initialization)

## Notes and Other Information
- Performs lazy initialization to handle timing dependencies with RecursiveUnion
- Uses parameter slots to communicate with ancestor RecursiveUnion nodes
- Assumes RecursiveUnion doesn't allow projection for tuple type compatibility
- Must complete projection info initialization before calling ExecScan
- The scan tuple type matches the RecursiveUnion's result rowtype
- Integrates with PostgreSQL's generic scanning framework via ExecScan

## Simplified Source

```c
static TupleTableSlot *
ExecWorkTableScan(PlanState *pstate)
{
    WorkTableScanState *node = castNode(WorkTableScanState, pstate);

    // Lazy initialization on first call
    if (node->rustate == NULL)
    {
        WorkTableScan *plan = (WorkTableScan *) node->ss.ps.plan;
        EState *estate = node->ss.ps.state;

        // Find ancestor RecursiveUnion state via parameter slot
        ParamExecData *param = &(estate->es_param_exec_vals[plan->wtParam]);
        node->rustate = castNode(RecursiveUnionState, DatumGetPointer(param->value));

        // Set scan tuple type to match RecursiveUnion result type
        ExecAssignScanType(&node->ss, ExecGetResultType(&node->rustate->ps));

        // Initialize projection info
        ExecAssignScanProjectionInfo(&node->ss);
    }

    // Delegate to generic scan framework
    return ExecScan(&node->ss, WorkTableScanNext, WorkTableScanRecheck);
}
```

This function implements worktable scanning by:
1. **Lazy Initialization**: Deferring setup until first call to handle timing dependencies
2. **State Location**: Finding the ancestor RecursiveUnion through parameter slots
3. **Type Setup**: Configuring scan tuple type to match RecursiveUnion output
4. **Projection Setup**: Initializing projection information for tuple processing
5. **Delegation**: Using the generic ExecScan framework with worktable-specific methods