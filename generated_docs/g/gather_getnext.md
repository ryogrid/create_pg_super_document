# gather_getnext

## Location
[src/backend/executor/nodeGather.c:256-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGather.c#L256-L303)

## Overview
The core tuple retrieval function for Gather nodes that implements the logic for reading tuples from multiple sources: worker processes via tuple queues and local plan execution.

## Definition

```c
static TupleTableSlot *
gather_getnext(GatherState *gatherstate)
```
## Detailed Description
gather_getnext implements the central tuple retrieval strategy for parallel query execution. It manages two possible sources of tuples: worker processes (accessed through tuple queues) and local execution of the child plan. The function operates in a loop, first attempting to read from worker processes using gather_readnext, and if no tuple is available from workers, it may execute the plan locally if need_to_scan_locally is true.

When reading from workers, tuples come as MinimalTuple objects that are stored in the funnel_slot for return to the caller. For local execution, the function temporarily installs the parallel DSA (Dynamic Shared Area) context to ensure proper memory management during parallel execution, then calls ExecProcNode on the child plan. The function handles the transition from having active workers to pure local execution by setting need_to_scan_locally to false once local scanning is complete.

## Parameters / Member Variables
- : The GatherState containing worker information, tuple queue readers, and configuration flags

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (accesses the child plan state)
  - CHECK_FOR_INTERRUPTS (allows query cancellation)
  - [gather_readnext](gather_readnext.md) (reads tuples from worker processes)
  - HeapTupleIsValid (checks if received tuple is valid)
  - [ExecStoreMinimalTuple](../E/ExecStoreMinimalTuple.md) (stores worker tuple in funnel slot)
  - [ExecProcNode](../E/ExecProcNode.md) (executes child plan locally)
  - TupIsNull (checks for end of local data)
  - [ExecClearTuple](../E/ExecClearTuple.md) (returns empty slot when no more data)
- Called from (representative examples):
  - [ExecGather](../E/ExecGather.md) (main execution function for Gather nodes)

## Notes and Other Information
- Implements a priority system: worker tuples are processed before local execution
- Uses the funnel_slot specifically for storing tuples received from workers
- Temporarily sets estate->es_query_dsa during local execution to maintain proper parallel execution context
- The loop continues until both worker sources and local execution are exhausted
- Worker processes are accessed through a round-robin strategy implemented in gather_readnext
- Sets need_to_scan_locally to false after local scanning completes to prevent repeated local scans
- Returns an empty slot when all data sources are exhausted, signaling end-of-data to the caller
- Critical for maintaining correct tuple ordering and ensuring all parallel data sources are properly consumed