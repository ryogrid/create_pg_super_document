# ReplicationSlotSave

## Location
[src/backend/replication/slot.c:992-1009](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L992-L1009)

## Overview
Serializes the currently acquired replication slot's state from memory to disk, ensuring the slot's current state survives a crash.

## Definition


## Detailed Description
This function provides a simple interface to persist the current replication slot to disk. It operates on the globally accessible MyReplicationSlot variable, which points to the slot currently acquired by the calling process. The function constructs the appropriate file system path and delegates the actual serialization work to SaveSlotToPath, using ERROR level for any failures that occur during the save operation.

This is a critical function for ensuring replication slot durability - without periodic saves, slot state changes (like advancing restart_lsn or confirmed_flush_lsn) would be lost on crash, potentially causing data inconsistency or requiring replication to restart from much earlier positions.

## Parameters / Member Variables
- No parameters (operates on the global MyReplicationSlot variable)

## Dependencies
- Functions called/Symbols referenced:
  - [SaveSlotToPath](../S/SaveSlotToPath.md)
- Called from (representative examples):
  - CreateInitDecodingContext
  - CreateDecodingContext
  - LogicalConfirmReceivedLocation
  - [ReplicationSlotAlter](ReplicationSlotAlter.md)
  - [ReplicationSlotPersist](ReplicationSlotPersist.md)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)
  - [synchronize_one_slot](../s/synchronize_one_slot.md)
  - [update_local_synced_slot](../u/update_local_synced_slot.md)

## Notes and Other Information
- Requires that MyReplicationSlot is not NULL (verified by Assert)
- Uses ERROR level for failures, meaning save failures will abort the current transaction
- File path follows the pattern "pg_replslot/{slot_name}"
- This is a synchronization point where in-memory slot changes become durable
- Called frequently during logical replication to ensure progress is not lost
- Essential for maintaining replication slot consistency across server restarts