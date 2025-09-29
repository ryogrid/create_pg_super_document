# ExecShutdownForeignScan

## Location
[src/backend/executor/nodeForeignscan.c:441-455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeForeignscan.c#L441-L455)

## Overview
Provides a mechanism for Foreign Data Wrappers (FDWs) to perform cleanup operations, stop asynchronous resource consumption, and release any resources still held when shutting down a foreign scan operation.

## Definition
```c
void ExecShutdownForeignScan(ForeignScanState *node)
```

## Detailed Description
This function serves as an interface between PostgreSQL's executor and FDW implementations to enable proper resource cleanup during foreign scan shutdown. It checks if the FDW provides a ShutdownForeignScan callback function and invokes it if available. This allows FDWs to perform any necessary cleanup operations such as closing connections, freeing memory, canceling ongoing asynchronous operations, or releasing other resources that were allocated during the scan's execution.

The function is part of PostgreSQL's extensible foreign data wrapper architecture, providing a clean shutdown hook that FDWs can implement to ensure proper resource management.

## Parameters / Member Variables
- `node`: A pointer to the ForeignScanState containing the state information for the foreign scan operation, including the FDW routine table

## Dependencies
- Functions called/Symbols referenced:
  - [ForeignScanState](../F/ForeignScanState.md) (structure)
  - [FdwRoutine](../F/FdwRoutine.md) (structure)
  - ShutdownForeignScan (FDW callback function)
- Called from (representative examples):
  - [ExecShutdownNode_walker](ExecShutdownNode_walker.md) (in execProcnode.c)

## Notes and Other Information
- This function is optional for FDWs to implement - it only calls the shutdown callback if the FDW provides one
- Part of the asynchronous foreign scan infrastructure introduced to support concurrent/parallel foreign scans
- Should be called during plan node shutdown to ensure proper cleanup of FDW resources
- Located in src/backend/executor/nodeForeignscan.c:441-455

## Simplified Source

```c
void ExecShutdownForeignScan(ForeignScanState *node) {
    // Get the FDW's routine table
    FdwRoutine *fdwroutine = node->fdwroutine;

    // Call shutdown callback if the FDW provides one
    if (fdwroutine->ShutdownForeignScan) {
        fdwroutine->ShutdownForeignScan(node);
    }
}
```