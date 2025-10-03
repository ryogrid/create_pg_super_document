# ExecFunctionScan

## Location
[src/backend/executor/nodeFunctionscan.c:265-278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeFunctionscan.c#L265-L278)

## Overview
ExecFunctionScan is the main execution function for function scan nodes that sequentially scans function results and returns qualifying tuples.

## Definition

```c
static TupleTableSlot *
ExecFunctionScan(PlanState *pstate)
```
## Detailed Description
ExecFunctionScan serves as the primary execution interface for function scan operations in PostgreSQL's executor. It acts as a thin wrapper around the generic ExecScan framework, providing the specialized access methods needed for function scanning:

1. **Delegation Pattern**: Rather than implementing scan logic directly, it delegates to the generic ExecScan routine, which handles common scanning concerns like qualification checking, projection, and tuple slot management.

2. **Access Method Integration**: It provides FunctionNext as the tuple access method and FunctionRecheck as the tuple recheck method, integrating function-specific logic into the standard scan framework.

3. **Type Safety**: Uses castNode to safely convert the generic PlanState to a FunctionScanState, ensuring type correctness.

This design follows PostgreSQL's consistent pattern across all scan node types, providing a uniform interface while allowing each scan type to customize its specific data access behavior.

## Parameters / Member Variables
- `*pstate`: Generic PlanState pointer that must be a FunctionScanState, containing the execution state for the function scan node
## Dependencies
- Functions called/Symbols referenced:
  - castNode (for type-safe conversion)
  - [ExecScan](ExecScan.md) (generic scan framework)
  - [FunctionNext](../F/FunctionNext.md) (tuple access method)
  - [FunctionRecheck](../F/FunctionRecheck.md) (tuple recheck method)
- Called from (representative examples):
  - [ExecInitFunctionScan](ExecInitFunctionScan.md) (via ps_ExecProcNode assignment)

## Notes and Other Information
- Follows the standard PostgreSQL executor pattern for scan nodes
- Provides a clean separation between generic scan logic and function-specific access methods
- The actual tuple retrieval and processing is handled by FunctionNext
- Integrates seamlessly with PostgreSQL's qualification testing and projection mechanisms
- Part of the executor's scan node hierarchy alongside table scans, index scans, etc.

## Simplified Source

```c
static TupleTableSlot *
ExecFunctionScan(PlanState *pstate)
{
    // Cast to function scan state
    FunctionScanState *node = castNode(FunctionScanState, pstate);

    // Delegate to generic scan framework with function-specific methods
    return ExecScan(&node->ss, FunctionNext, FunctionRecheck);
}
```