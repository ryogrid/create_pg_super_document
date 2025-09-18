# process_syncing_tables_for_sync

## Location
[src/backend/replication/logical/tablesync.c:295-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/tablesync.c#L295-L417)

## Overview
Handles the transition of a table synchronization worker from CATCHUP state to SYNCDONE when it reaches the predetermined synchronization point in the WAL stream.

## Definition
```c
static void process_syncing_tables_for_sync(XLogRecPtr current_lsn)
```

## Detailed Description
This function is responsible for completing table synchronization in logical replication. It runs in table synchronization workers and monitors whether the worker has caught up to the predetermined WAL position. When the current LSN reaches or exceeds the target synchronization LSN, it performs the final steps to mark the table as fully synchronized.

The function performs these critical operations:
1. Checks if the worker is in CATCHUP state and has reached the synchronization point
2. Updates the worker state to SYNCDONE and persists it in the catalog
3. Terminates WAL streaming and cleans up the synchronization replication slot
4. Removes the tablesync origin tracking to avoid conflicts
5. Calls `finish_sync_worker()` to terminate the worker

The function includes careful transaction management and error handling to ensure that cleanup operations complete successfully and don't leave orphaned resources.

## Parameters / Member Variables
- `current_lsn`: Current LSN position in the WAL stream to compare against the synchronization target

## Dependencies
- Functions called/Symbols referenced:
  - `SpinLockAcquire()`/`SpinLockRelease()`
  - [IsTransactionState](../I/IsTransactionState.md)()
  - [StartTransactionCommand](../S/StartTransactionCommand.md)()
  - [UpdateSubscriptionRelState](../U/UpdateSubscriptionRelState.md)()
  - `walrcv_endstreaming()`
  - [ReplicationSlotNameForTablesync](../R/ReplicationSlotNameForTablesync.md)()
  - [ReplicationSlotDropAtPubNode](../R/ReplicationSlotDropAtPubNode.md)()
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)()
  - [pgstat_report_stat](pgstat_report_stat.md)()
  - [ReplicationOriginNameForLogicalRep](../R/ReplicationOriginNameForLogicalRep.md)()
  - [replorigin_session_reset](../r/replorigin_session_reset.md)()
  - [replorigin_drop_by_name](../r/replorigin_drop_by_name.md)()
  - `finish_sync_worker()`
  - Constants: `SUBREL_STATE_CATCHUP`, `SUBREL_STATE_SYNCDONE`, `NAMEDATALEN`, `InvalidRepOriginId`
- Called from (representative examples):
  - [process_syncing_tables](process_syncing_tables.md) (src/backend/replication/logical/tablesync.c:707)

## Notes and Other Information
- This function implements the final phase of table synchronization in PostgreSQL logical replication
- Uses spinlocks to protect access to shared worker state (`MyLogicalRepWorker->relstate`)
- Performs extensive cleanup including slot dropping and origin removal to prevent resource leaks
- Error handling ensures that failed cleanup doesn't prevent proper state transitions
- The function starts two separate transactions: one for state update, another for origin cleanup
- Critical for maintaining consistency between the synchronized table and ongoing replication
- Part of the table synchronization state machine that coordinates between sync and apply workers
- The function is static, used only within the tablesync.c module for WORKERTYPE_TABLESYNC workers