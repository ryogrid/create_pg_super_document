# SyncReplicationSlots

## Location
[src/backend/replication/logical/slotsync.c:1725-1742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L1725-L1742)

## Overview
Main function that synchronizes failover-enabled replication slots from a primary server to a standby server using the specified WAL receiver connection.

## Definition
```c
void SyncReplicationSlots(WalReceiverConn *wrconn)
```

## Detailed Description
This function orchestrates the complete process of synchronizing replication slots from a primary PostgreSQL server to a standby server. It serves as the main entry point for slot synchronization operations, whether called from the slot sync worker process or from the `pg_sync_replication_slots()` SQL function.

The function implements a robust error-handling framework using PostgreSQL's `PG_ENSURE_ERROR_CLEANUP` mechanism to guarantee proper resource cleanup even if errors occur during synchronization. The synchronization process follows these key steps:

1. **Validation and Setup**: Checks for concurrent operations and promotion conflicts, then sets synchronization flags
2. **Remote Information Validation**: Validates the connection and remote server information
3. **Slot Synchronization**: Fetches failover slots from primary and creates/updates them locally
4. **Cleanup**: Removes temporary slots and resets synchronization flags

The function ensures atomic operation by preventing concurrent slot synchronization attempts and properly handling the race condition during standby promotion scenarios.

## Parameters / Member Variables
- `wrconn`: A valid WalReceiverConn pointer representing the connection to the primary server for fetching slot information

## Dependencies
- Functions called/Symbols referenced:
  - PG_ENSURE_ERROR_CLEANUP (error handling framework)
  - [slotsync_failure_callback](../s/slotsync_failure_callback.md) (error cleanup callback)
  - [check_and_set_sync_info](../c/check_and_set_sync_info.md) (validates state and sets sync flags)
  - validate_remote_info (validates remote connection)
  - synchronize_slots (performs actual slot synchronization)
  - [ReplicationSlotCleanup](../R/ReplicationSlotCleanup.md) (cleans up temporary slots)
  - [reset_syncing_flag](../r/reset_syncing_flag.md) (resets synchronization flags)
  - PG_END_ENSURE_ERROR_CLEANUP (completes error handling block)
- Called from (representative examples):
  - [pg_sync_replication_slots](../p/pg_sync_replication_slots.md) (SQL function interface)

## Notes and Other Information
- Uses PostgreSQL's error cleanup framework to ensure proper resource management
- Prevents concurrent slot synchronization by checking and setting shared memory flags
- Handles both manual invocation via SQL function and automatic worker-based synchronization
- Critical for logical replication failover scenarios in PostgreSQL physical standby setups
- The error callback ensures proper cleanup of slots, connections, and synchronization state
- Part of the logical replication slot synchronization infrastructure introduced for failover support
- Located in src/backend/replication/logical/slotsync.c:1720-1742