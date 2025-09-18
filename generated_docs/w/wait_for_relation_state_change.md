# wait_for_relation_state_change

## Location
src/backend/replication/logical/tablesync.c: 184 - 231

## Overview
Waits until a relation's synchronization state in the subscription catalog matches the expected state, used during logical replication table synchronization.

## Definition
```c
static bool wait_for_relation_state_change(Oid relid, char expected_state)
```

## Detailed Description
This function implements a polling mechanism that continuously monitors the synchronization state of a specific relation in logical replication. It runs in an infinite loop, periodically checking the catalog to determine if a table has reached the expected synchronization state.

The function is primarily used in the apply worker when transitioning from CATCHUP state to SYNCDONE during logical replication table synchronization. It ensures proper coordination between different replication workers by waiting for state transitions to complete.

The function performs the following operations in each iteration:
1. Checks for interrupts to allow graceful shutdown
2. Invalidates catalog snapshots to get fresh data
3. Queries the current subscription relation state
4. Verifies the sync worker is still running
5. Waits using a latch with timeout if state hasn't changed

## Parameters / Member Variables
- `relid`: OID of the relation to monitor for state changes
- `expected_state`: Character representing the expected synchronization state (typically SUBREL_STATE_SYNCDONE)

## Dependencies
- Functions called/Symbols referenced:
  - `CHECK_FOR_INTERRUPTS()`
  - [InvalidateCatalogSnapshot](../I/InvalidateCatalogSnapshot.md)()
  - [GetSubscriptionRelState](../G/GetSubscriptionRelState.md)()
  - `LWLockAcquire()`
  - `logicalrep_worker_find()`
  - `LWLockRelease()`
  - [WaitLatch](../W/WaitLatch.md)()
  - [ResetLatch](../R/ResetLatch.md)()
  - `SUBREL_STATE_UNKNOWN`
  - [LogicalRepWorker](../L/LogicalRepWorker.md)
- Called from (representative examples):
  - [tablesync_start_time_mapping](../t/tablesync_start_time_mapping.md) (src/backend/replication/logical/tablesync.c:588)

## Notes and Other Information
- Returns `true` when the relation reaches the expected state
- Returns `false` if the table sync worker disappears, the table itself disappears, or the table state is reset to UNKNOWN
- Uses a 1-second timeout on the latch wait to avoid blocking indefinitely
- Critical for ensuring proper synchronization coordination in logical replication
- Part of PostgreSQL's logical replication infrastructure for maintaining table sync state consistency
- The function is static, indicating it's only used within the tablesync.c module