# pa_wait_for_xact_state

## Location
[src/backend/replication/logical/applyparallelworker.c:1244-1273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L1244-L1273)

## Overview
Waits until a parallel apply workers transaction state reaches or exceeds a specified target state, providing synchronization between the leader and parallel workers.

## Definition
```c
static void pa_wait_for_xact_state(ParallelApplyWorkerInfo *winfo, ParallelTransState xact_state)
```

## Detailed Description
pa_wait_for_xact_state implements a polling-based synchronization mechanism for coordinating transaction states between the leader process and parallel apply workers in logical replication. The function continuously checks the current transaction state of the specified parallel worker and waits until it reaches or exceeds the desired state. It uses the WaitLatch mechanism with a timeout to efficiently wait for state changes, periodically checking for interrupts and resetting the latch to prevent spinning. This function is crucial for ensuring proper ordering and coordination of transaction processing across parallel workers.

## Parameters / Member Variables
- `winfo`: ParallelApplyWorkerInfo structure containing information about the parallel worker and its shared state
- `xact_state`: Target ParallelTransState that the worker should reach or exceed before the function returns

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelApplyWorkerInfo](../P/ParallelApplyWorkerInfo.md)
  - ParallelTransState
  - [pa_get_xact_state](pa_get_xact_state.md)
  - [WaitLatch](../W/WaitLatch.md)
  - WL_LATCH_SET
  - WL_TIMEOUT
  - WL_EXIT_ON_PM_DEATH
  - [ResetLatch](../R/ResetLatch.md)
  - MyLatch
  - CHECK_FOR_INTERRUPTS

- Called from (representative examples):
  - [pa_wait_for_xact_finish](pa_wait_for_xact_finish.md)

## Notes and Other Information
- Uses an infinite loop with periodic checks rather than event-driven waiting
- Waits for 10 milliseconds on each iteration (10L parameter to WaitLatch)
- Uses WAIT_EVENT_LOGICAL_PARALLEL_APPLY_STATE_CHANGE as the wait event type for monitoring
- Includes WL_EXIT_ON_PM_DEATH flag to handle postmaster death scenarios
- Resets the latch after each wait to prevent unnecessary spinning
- Checks for interrupts on each iteration to handle cancellation requests
- The comparison uses >= operator, meaning the function returns when the state equals or exceeds the target
- Static function, indicating its an internal implementation detail of the parallel apply system