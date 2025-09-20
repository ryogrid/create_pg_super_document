# ExecEndForeignScan

## Location
[src/backend/executor/nodeForeignscan.c:297-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeForeignscan.c#L297-L322)

## Overview
ExecEndForeignScan performs cleanup and shutdown operations for a foreign scan node, delegating to FDW-specific cleanup routines and handling any outer plan nodes.

## Definition

```c
void
ExecEndForeignScan(ForeignScanState *node)
```
## Detailed Description
ExecEndForeignScan serves as the cleanup and shutdown function for foreign scan operations in PostgreSQL's executor framework. This function is responsible for properly terminating foreign scan execution by ensuring all resources are freed and all FDW-specific cleanup is performed.

The function follows a structured cleanup approach:

**FDW-Specific Cleanup**: The primary responsibility is delegating to the appropriate FDW shutdown routine. For SELECT operations, it calls the FDW's EndForeignScan callback to allow the FDW to clean up scan-specific resources such as connection pools, cursors, or cached data. For direct modification operations (INSERT, UPDATE, DELETE), it calls EndDirectModify, but only when not in EvalPlanQual mode since direct modifications are not initialized during EvalPlanQual processing.

**Plan Tree Cleanup**: After FDW cleanup, the function handles any outer plan nodes by recursively calling ExecEndNode. This ensures proper cleanup of complex query structures where foreign scans may be part of larger execution trees involving joins, subqueries, or other plan nodes.

The function maintains consistency with the initialization and execution phases by applying the same EvalPlanQual logic - direct modification operations are only cleaned up when es_epq_active is NULL, mirroring the conditional initialization in ExecInitForeignScan.

## Parameters / Member Variables
- : ForeignScanState structure containing the execution state to be cleaned up, including FDW routines, plan information, and any outer plan state

## Dependencies
- Functions called/Symbols referenced:
  - EndForeignScan (via fdwroutine callback)
  - EndDirectModify (via fdwroutine callback)
  - outerPlanState
  - [ExecEndNode](ExecEndNode.md)
- Called from:
  - [ExecEndNode](ExecEndNode.md)

## Notes and Other Information
- This function is void, indicating it performs cleanup without returning status information
- The EvalPlanQual check for direct modifications ensures consistency with initialization behavior
- FDW cleanup is performed before outer plan cleanup to maintain proper resource deallocation order
- The function assumes that FDW routines properly handle NULL or invalid states gracefully
- Outer plan cleanup is conditional on the existence of outer plans, preventing unnecessary calls
- No explicit memory context cleanup is performed as this is handled by the broader executor framework
- The function complements ExecInitForeignScan by providing symmetric cleanup operations