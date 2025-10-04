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

## Simplified Source

```c
static bool
update_local_synced_slot(RemoteSlot *remote_slot, Oid remote_dbid,
						 bool *found_consistent_snapshot,
						 bool *remote_slot_precedes)
{
	ReplicationSlot *slot = MyReplicationSlot;
	bool updated_xmin_or_lsn = false;
	bool updated_config = false;

	// Initialize output parameters
	if (found_consistent_snapshot)
		*found_consistent_snapshot = false;
	if (remote_slot_precedes)
		*remote_slot_precedes = false;

	// Check if remote slot is behind local slot (potential data loss)
	if (remote_slot->restart_lsn < slot->data.restart_lsn ||
		TransactionIdPrecedes(remote_slot->catalog_xmin, slot->data.catalog_xmin)) {

		// Log warning about potential data loss
		ereport(slot->data.persistency == RS_TEMPORARY ? LOG : DEBUG1,
				errmsg("could not synchronize replication slot \"%s\"", remote_slot->name),
				errdetail("Synchronization could lead to data loss..."));

		if (remote_slot_precedes)
			*remote_slot_precedes = true;
	}
	// Update LSNs and xmins if remote slot is ahead
	else if (remote_slot->confirmed_lsn > slot->data.confirmed_flush ||
			 remote_slot->restart_lsn > slot->data.restart_lsn ||
			 TransactionIdFollows(remote_slot->catalog_xmin, slot->data.catalog_xmin)) {

		// Direct update if serialized snapshot exists
		if (SnapBuildSnapshotExists(remote_slot->restart_lsn)) {
			SpinLockAcquire(&slot->mutex);
			slot->data.restart_lsn = remote_slot->restart_lsn;
			slot->data.confirmed_flush = remote_slot->confirmed_lsn;
			slot->data.catalog_xmin = remote_slot->catalog_xmin;
			SpinLockRelease(&slot->mutex);

			if (found_consistent_snapshot)
				*found_consistent_snapshot = true;
		} else {
			// Use slot advancement machinery for proper state updates
			LogicalSlotAdvanceAndCheckSnapState(remote_slot->confirmed_lsn,
												found_consistent_snapshot);
		}
		updated_xmin_or_lsn = true;
	}

	// Update configuration if different
	if (remote_dbid != slot->data.database ||
		remote_slot->two_phase != slot->data.two_phase ||
		remote_slot->failover != slot->data.failover ||
		strcmp(remote_slot->plugin, NameStr(slot->data.plugin)) != 0) {

		NameData plugin_name;
		namestrcpy(&plugin_name, remote_slot->plugin);

		SpinLockAcquire(&slot->mutex);
		slot->data.plugin = plugin_name;
		slot->data.database = remote_dbid;
		slot->data.two_phase = remote_slot->two_phase;
		slot->data.failover = remote_slot->failover;
		SpinLockRelease(&slot->mutex);

		updated_config = true;
	}

	// Persist changes to disk
	if (updated_config || updated_xmin_or_lsn) {
		ReplicationSlotMarkDirty();
		ReplicationSlotSave();
	}

	// Update global xmin tracking
	if (updated_xmin_or_lsn) {
		SpinLockAcquire(&slot->mutex);
		slot->effective_catalog_xmin = remote_slot->catalog_xmin;
		SpinLockRelease(&slot->mutex);

		ReplicationSlotsComputeRequiredXmin(false);
		ReplicationSlotsComputeRequiredLSN();
	}

	return updated_config || updated_xmin_or_lsn;
}
```