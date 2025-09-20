# ExecEndModifyTable

## Location
[src/backend/executor/nodeModifyTable.c:4907-4960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L4907-L4960)

## Overview
Performs cleanup and shutdown operations for a ModifyTable execution node, releasing all allocated resources including FDW connections, tuple routing structures, and EPQ state.

## Definition

```c
void
ExecEndModifyTable(ModifyTableState *node)
```
## Detailed Description
This function systematically cleans up all resources allocated during the initialization and execution of a ModifyTable node. It ensures proper shutdown of foreign data wrappers, cleanup of partitioning structures, termination of EPQ (EvalPlanQual) state, and shutdown of the associated subplan.

The cleanup process follows a specific order:
1. **FDW Shutdown**: Calls EndForeignModify for all foreign data wrappers that support it
2. **Batch Slot Cleanup**: Releases batch processing slots used by FDWs that support batching
3. **Partition Cleanup**: Closes all partitioned tables, leaf partitions, and their indices via ExecCleanupTupleRouting
4. **Tuple Slot Cleanup**: Releases the root tuple slot used for tuple routing
5. **EPQ Termination**: Ends any active EPQ (EvalPlanQual) execution state
6. **Subplan Shutdown**: Recursively shuts down the underlying subplan

## Parameters / Member Variables
- : ModifyTableState structure containing all the execution state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ExecDropSingleTupleTableSlot](ExecDropSingleTupleTableSlot.md)
  - [ExecCleanupTupleRouting](ExecCleanupTupleRouting.md)
  - [EvalPlanQualEnd](EvalPlanQualEnd.md)
  - [ExecEndNode](ExecEndNode.md)
  - outerPlanState
- Data structures used:
  - [ModifyTableState](../M/ModifyTableState.md)
  - [ResultRelInfo](../R/ResultRelInfo.md)
- Called from:
  - [ExecEndNode](ExecEndNode.md)

## Notes and Other Information
- Must be called to properly clean up resources allocated by ExecInitModifyTable
- Handles cleanup for all types of result relations including regular tables, foreign tables, and partitioned tables
- Safely handles cases where certain features weren't used (e.g., no partition routing, no FDW batching)
- The cleanup is performed in reverse order of initialization to ensure proper dependency management
- Critical for preventing resource leaks in long-running transactions or when many ModifyTable operations are performed
- Part of PostgreSQL's standard executor cleanup protocol where every ExecInit* function has a corresponding ExecEnd* function