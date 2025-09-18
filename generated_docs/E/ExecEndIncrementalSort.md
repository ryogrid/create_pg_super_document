# ExecEndIncrementalSort

## Location
[src/backend/executor/nodeIncrementalSort.c:1077-1106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIncrementalSort.c#L1077-L1106)

## Overview
Shuts down an incremental sort node by releasing all allocated resources including tuple slots, tuplesort states, and the outer child node.

## Definition


## Detailed Description
ExecEndIncrementalSort performs cleanup and resource deallocation for an incremental sort node when query execution is complete or the node is no longer needed. This function ensures proper cleanup of all resources allocated during the node's lifetime, including standalone tuple table slots, tuplesort states for both full and prefix sorting operations, and the outer child node.

The cleanup process follows a specific order:
1. Release standalone tuple slots (group_pivot and transfer_tuple)
2. End and deallocate tuplesort states for both fullsort and prefixsort operations
3. Shut down the outer child node recursively

This systematic cleanup prevents memory leaks and ensures all associated resources are properly returned to the system.

## Parameters / Member Variables
- : The IncrementalSortState containing all runtime state and resources to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ExecDropSingleTupleTableSlot](ExecDropSingleTupleTableSlot.md) (releases standalone tuple slots)
  - tuplesort_end (ends tuplesort operations and frees memory)
  - [ExecEndNode](ExecEndNode.md) (recursively shuts down outer child node)
  - outerPlanState (accesses outer child plan state)
  - SO_printf (debug output)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (main node cleanup dispatcher)

## Notes and Other Information
- The function safely handles cases where tuplesort states may be NULL (not yet initialized or already cleaned up)
- Standalone tuple slots created with MakeSingleTupleTableSlot must be explicitly released with ExecDropSingleTupleTableSlot
- The cleanup order is important: local resources are freed before shutting down child nodes
- This function is part of the standard PostgreSQL executor node cleanup protocol
- Debug output is provided via SO_printf to trace node shutdown process