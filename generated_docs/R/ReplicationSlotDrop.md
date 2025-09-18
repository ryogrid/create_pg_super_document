# ReplicationSlotDrop

## Location
[src/backend/replication/slot.c:784-806](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L784-L806)

## Overview
Permanently drops a replication slot identified by name, with safety checks to prevent dropping slots that are currently being synchronized from a primary server.

## Definition
void ReplicationSlotDrop(const char *name, bool nowait)

## Detailed Description
ReplicationSlotDrop provides a high-level interface for permanently removing a replication slot from the system. The function performs several important operations:

1. Acquires the specified slot by name using the nowait parameter to control blocking behavior
2. Validates that the slot can be safely dropped by checking if it's a synced slot during recovery
3. Prevents accidental deletion of slots that are being synchronized from a primary server to a standby
4. Delegates the actual dropping operation to ReplicationSlotDropAcquired()

The function includes important safety mechanisms to prevent data loss scenarios where synchronized slots are accidentally removed on standby servers.

## Parameters / Member Variables
- : The name of the replication slot to drop
- : Boolean flag controlling whether to wait for slot acquisition - if true, the function will not block if the slot is currently in use by another process

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotAcquire](ReplicationSlotAcquire.md)
  - [RecoveryInProgress](RecoveryInProgress.md)
  - [ReplicationSlotDropAcquired](ReplicationSlotDropAcquired.md)
- Called from (representative examples):
  - [pg_drop_replication_slot](../p/pg_drop_replication_slot.md)
  - [DropReplicationSlot](../D/DropReplicationSlot.md)

## Notes and Other Information
- Requires that MyReplicationSlot be NULL before calling (no slot currently held)
- Includes special protection for synced slots during recovery to prevent accidental deletion
- The function will raise an ERROR if attempting to drop a synced slot during recovery
- This is the standard entry point for dropping replication slots by name
- Properly handles both blocking and non-blocking slot acquisition modes
- Used by SQL functions and replication protocol commands for slot management