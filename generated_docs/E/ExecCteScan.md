# ExecCteScan

## Location
[src/backend/executor/nodeCtescan.c:160-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeCtescan.c#L160-L174)

## Overview
ExecCteScan is the main execution function for CTE (Common Table Expression) scans that returns the next qualifying tuple by delegating to the generic ExecScan framework with CTE-specific access methods.

## Definition

```c
static TupleTableSlot *
ExecCteScan(PlanState *pstate)
```
## Detailed Description
ExecCteScan serves as the primary entry point for executing CTE scan operations within PostgreSQL's executor framework. Rather than implementing scan logic directly, it follows the standard executor pattern by calling the generic ExecScan() function and providing CTE-specific access method functions.

The function acts as a thin wrapper that:
1. Casts the generic PlanState to the specific CteScanState type
2. Delegates to ExecScan() with CteScanNext as the tuple retrieval method and CteScanRecheck as the EvalPlanQual recheck method
3. Returns the tuple slot containing the next qualifying tuple from the CTE

This design allows CTE scans to benefit from the common scan infrastructure (including qualification checking, projection, etc.) while providing specialized tuple access logic through the callback functions.

## Parameters / Member Variables
- : PlanState pointer that must be castable to CteScanState, containing the CTE scan execution state

## Dependencies
- Functions called/Symbols referenced:
  - castNode: Safely cast PlanState to CteScanState
  - [ExecScan](ExecScan.md): Generic scan execution framework
  - [CteScanNext](../C/CteScanNext.md): CTE-specific tuple retrieval method (passed as callback)
  - [CteScanRecheck](../C/CteScanRecheck.md): CTE-specific tuple recheck method for EvalPlanQual (passed as callback)
- Called from (representative examples):
  - [ExecInitCteScan](ExecInitCteScan.md): Sets this function as the execution method during node initialization

## Notes and Other Information
- Follows the standard PostgreSQL executor pattern of delegating to ExecScan with specialized access methods
- The actual tuple retrieval logic is implemented in CteScanNext, while this function handles the executor framework integration
- Part of the plan node execution interface where each node type provides an execution function
- Located at src/backend/executor/nodeCtescan.c:160-174

## Simplified Source

```c
static TupleTableSlot *
ExecCteScan(PlanState *pstate)
{
    // Cast to CTE scan state
    CteScanState *node = castNode(CteScanState, pstate);

    // Delegate to generic scan framework with CTE-specific methods
    return ExecScan(&node->ss, CteScanNext, CteScanRecheck);
}
```