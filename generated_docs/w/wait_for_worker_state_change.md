# wait_for_worker_state_change

## Location
[src/backend/replication/logical/tablesync.c:232-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/tablesync.c#L232-L280)

## Overview
Waits until the apply worker changes the state of the current synchronization worker to the expected state, facilitating coordination between workers in logical replication.

## Definition
```c
static bool wait_for_worker_state_change(char expected_state)
```

## Detailed Description
This function implements a coordination mechanism between logical replication workers. It runs in a synchronization worker and waits for the apply worker to change its state to the expected value. The function is specifically used when transitioning from SYNCWAIT state to CATCHUP state during table synchronization.

The function performs the following operations in each iteration:
1. Checks for interrupts to allow graceful shutdown
2. Atomically reads the current worker state from `MyLogicalRepWorker->relstate`
3. If already in the expected state, returns immediately
4. Looks up the apply worker and signals it if found
5. Waits using a latch for the apply worker to notify state change

The function includes error handling for cases where the apply worker disappears, ensuring that synchronization workers don't wait indefinitely for a non-existent coordinator.

## Parameters / Member Variables
- `expected_state`: Character representing the expected worker state (typically SUBREL_STATE_CATCHUP)

## Dependencies
- Functions called/Symbols referenced:
  - `CHECK_FOR_INTERRUPTS()`
  - `LWLockAcquire()`
  - `logicalrep_worker_find()`
  - `logicalrep_worker_wakeup_ptr()`
  - `LWLockRelease()`
  - [WaitLatch](../W/WaitLatch.md)()
  - [ResetLatch](../R/ResetLatch.md)()
  - [LogicalRepWorker](../L/LogicalRepWorker.md)
  - `MyLogicalRepWorker`
  - `InvalidOid`
- Called from (representative examples):
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md) (src/backend/replication/logical/tablesync.c:1585)

## Notes and Other Information
- Returns `true` when the worker reaches the expected state
- Returns `false` if the apply worker has disappeared
- Uses a 1-second timeout on the latch wait to detect disappeared apply workers
- Actively signals the apply worker to notify it's waiting, improving responsiveness
- Part of the state machine coordination in PostgreSQL's logical replication table synchronization
- The function is static, used only within the tablesync.c module
- Critical for ensuring proper handoff between SYNCWAIT and CATCHUP phases during table sync
- Assumes that reading `MyLogicalRepWorker->relstate` is atomic enough to be done without locks