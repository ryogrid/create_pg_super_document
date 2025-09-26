# slotsync_failure_callback

## Location
[src/backend/replication/logical/slotsync.c:1688-1724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L1688-L1724)

## Overview
Error cleanup callback function that handles proper resource cleanup when slot synchronization operations fail or encounter errors.

## Definition
```c
static void slotsync_failure_callback(int code, Datum arg)
```

## Detailed Description
This static callback function performs critical cleanup operations when slot synchronization encounters errors or exceptions. It ensures that replication slots, connections, and synchronization state are properly cleaned up to prevent resource leaks and inconsistent state.

The function performs several essential cleanup tasks:
1. Releases any active replication slots that may be held by the current process
2. Cleans up temporary synced slots to avoid leaving dangling resources
3. Resets both local and shared memory synchronization flags if the process had them set
4. Disconnects the WAL receiver connection used for communication with the primary server

This cleanup is particularly important because the startup process during promotion waits for slot sync operations to complete by checking the 'syncing' flag, so proper cleanup ensures the system can proceed correctly during failover scenarios.

## Parameters / Member Variables
- `code`: Error code indicating the type of failure that triggered the callback
- `arg`: Datum containing a pointer to the WalReceiverConn that needs cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [WalReceiverConn](../W/WalReceiverConn.md) (connection structure type)
  - [ReplicationSlotRelease](../R/ReplicationSlotRelease.md) (releases active slots)
  - [ReplicationSlotCleanup](../R/ReplicationSlotCleanup.md) (cleans up temporary slots)
  - [reset_syncing_flag](../r/reset_syncing_flag.md) (resets synchronization flags)
  - walrcv_disconnect (disconnects WAL receiver connection)
- Called from (representative examples):
  - [SyncReplicationSlots](../S/SyncReplicationSlots.md) (registered as error callback)

## Notes and Other Information
- This is a static function, only accessible within the slotsync.c file
- Follows the PostgreSQL error callback function signature pattern
- Critical for preventing resource leaks during slot synchronization failures
- Ensures proper coordination with startup process during promotion scenarios
- The cleanup logic mirrors similar patterns used in WalSndErrorCleanup()
- Located in src/backend/replication/logical/slotsync.c:1684-1718