# ExecWorkTableScan

## Location
src/backend/executor/nodeWorktablescan.c: 81 - 129

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
  - ExecAssignScanType (assigns scan tuple descriptor)
  - [ExecGetResultType](ExecGetResultType.md) (gets result tuple type from plan state)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md) (initializes projection information)
  - [ExecScan](ExecScan.md) (generic scan execution framework)
  - [WorkTableScanNext](../W/WorkTableScanNext.md) (access method for next tuple)
  - [WorkTableScanRecheck](../W/WorkTableScanRecheck.md) (recheck method for EvalPlanQual)
- Types used:
  - [WorkTableScanState](../W/WorkTableScanState.md) (scan execution state)
  - WorkTableScan (plan node structure)
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