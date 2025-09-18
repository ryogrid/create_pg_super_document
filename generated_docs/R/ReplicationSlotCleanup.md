# ReplicationSlotCleanup

## Location
[src/backend/replication/slot.c:745-783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L745-L783)

## Overview
Cleans up temporary replication slots created in the current session, with an option to clean up only synced temporary slots or all temporary slots.

## Definition
void ReplicationSlotCleanup(bool synced_only)

## Detailed Description
ReplicationSlotCleanup iterates through all replication slots in the system and removes temporary slots that were created by the current process. The function provides flexibility through the synced_only parameter:

- When synced_only is true, it only removes temporary slots that are marked as synced
- When synced_only is false, it removes all temporary slots owned by the current process

The function uses a restart mechanism to handle the case where slots are dropped during iteration, which requires reacquiring locks and starting the search over. This ensures safe cleanup even when the slot array structure changes during processing.

## Parameters / Member Variables
- : Boolean flag that determines the scope of cleanup - if true, only synced temporary slots are cleaned up; if false, all temporary slots owned by the process are cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotDropPtr](ReplicationSlotDropPtr.md)
  - ConditionVariableBroadcast
  - LW_SHARED (lock mode)
  - RS_TEMPORARY (slot persistency type)
- Called from (representative examples):
  - [slotsync_worker_onexit](../s/slotsync_worker_onexit.md)
  - [ReplicationSlotShmemExit](ReplicationSlotShmemExit.md)
  - [WalSndErrorCleanup](../W/WalSndErrorCleanup.md)
  - [PostgresMain](../P/PostgresMain.md)
  - SyncReplicationSlots

## Notes and Other Information
- Only operates on temporary slots (RS_TEMPORARY persistency)
- Uses a restart loop to handle concurrent modifications to the slot array
- Requires that MyReplicationSlot be NULL before calling
- Properly manages locking to avoid deadlocks during slot cleanup
- Broadcasts condition variables to wake up processes waiting on cleaned-up slots
- Primarily used for session cleanup and error recovery scenarios