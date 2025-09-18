# ExecSubqueryScan

## Location
src/backend/executor/nodeSubqueryscan.c: 83 - 96

## Overview
ExecSubqueryScan is the main execution function for subquery scan operations, scanning the subquery sequentially and returning the next qualifying tuple.

## Definition
```c
static TupleTableSlot *ExecSubqueryScan(PlanState *pstate)
```

## Detailed Description
ExecSubqueryScan serves as the primary execution entry point for subquery scan nodes in PostgreSQL's executor. It implements the standard scan pattern by delegating to the generic ExecScan() routine, providing it with appropriate access method functions specific to subquery operations. The function follows PostgreSQL's executor architecture pattern where each node type provides a standardized interface while customizing behavior through function pointers. This design enables consistent handling of scanning operations across different node types while allowing for type-specific optimizations.

## Parameters / Member Variables
- `pstate`: A PlanState pointer that is cast to SubqueryScanState, containing the execution state and context for the subquery scan operation

## Dependencies
- Functions called/Symbols referenced:
  - castNode (safely casts PlanState to SubqueryScanState)
  - [ExecScan](ExecScan.md) (generic scan execution framework)
  - [SubqueryNext](../S/SubqueryNext.md) (access method for retrieving next tuple)
  - [SubqueryRecheck](../S/SubqueryRecheck.md) (recheck method for EvalPlanQual operations)
  - [SubqueryScanState](../S/SubqueryScanState.md) (execution state structure)
- Called from (representative examples):
  - [ExecInitSubqueryScan](ExecInitSubqueryScan.md) (during subquery scan node initialization)

## Notes and Other Information
- The function is marked as static, indicating it's only used within the nodeSubqueryScan.c file
- Uses the standard PostgreSQL executor pattern of delegating to ExecScan() with type-specific access methods
- Function pointers are cast to ExecScanAccessMtd and ExecScanRecheckMtd types for type safety
- Part of the executor node infrastructure that enables consistent scan operations across different plan node types
- Located at src/backend/executor/nodeSubqueryscan.c:83-96