# update_local_synced_slot

## Location
[src/backend/replication/logical/slotsync.c:168-332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L168-L332)

## Overview
Updates the local synced slot's metadata based on data from the remote slot, ensuring consistency and preventing data loss during slot synchronization.

## Definition

```c
static bool
update_local_synced_slot(RemoteSlot *remote_slot, Oid remote_dbid,
						 bool *found_consistent_snapshot,
						 bool *remote_slot_precedes)
```
## Detailed Description
This function synchronizes a local replication slot with its corresponding remote slot by updating LSNs, xmins, and configuration parameters. It implements careful logic to prevent data loss by ensuring that slot updates only proceed when consistent snapshots are available. The function handles two main scenarios:

1. **Remote slot precedes local slot**: When the remote slot needs WAL at positions earlier than what the local slot has, indicating potential data loss if synchronized.

2. **Remote slot is ahead**: When the remote slot has advanced beyond the local slot, requiring updates to LSNs and xmins while maintaining consistency.

The function uses either direct updates (when serialized snapshots exist) or slot advancement machinery to ensure proper snapbuilder and slot status updates.

## Parameters / Member Variables
- `*remote_slot`: Pointer to the remote slot structure containing the target state
- `remote_dbid`: Database OID associated with the remote slot
- `*found_consistent_snapshot`: Output parameter indicating if decoding can reach a consistent snapshot
- `*remote_slot_precedes`: Output parameter indicating if remote slot's position precedes local reserved position
## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdFollows](../T/TransactionIdFollows.md)
  - [SnapBuildSnapshotExists](../S/SnapBuildSnapshotExists.md)
  - [LogicalSlotAdvanceAndCheckSnapState](../L/LogicalSlotAdvanceAndCheckSnapState.md)
  - [ReplicationSlotMarkDirty](../R/ReplicationSlotMarkDirty.md)
  - [ReplicationSlotSave](../R/ReplicationSlotSave.md)
  - [ReplicationSlotsComputeRequiredXmin](../R/ReplicationSlotsComputeRequiredXmin.md)
  - [ReplicationSlotsComputeRequiredLSN](../R/ReplicationSlotsComputeRequiredLSN.md)
  - [namestrcpy](../n/namestrcpy.md)
- Called from:
  - [update_and_persist_local_synced_slot](update_and_persist_local_synced_slot.md)
  - [synchronize_one_slot](../s/synchronize_one_slot.md)

## Notes and Other Information
- Returns false if no update was needed (remote and local slot data identical), true otherwise
- Uses different logging levels (LOG vs DEBUG1) based on slot persistency to avoid log spam
- Implements spinlock protection when updating slot data structures
- Ensures xmin changes are written to disk before updating in-memory values to maintain crash safety
- Handles both temporary and persistent slots with appropriate error handling and logging