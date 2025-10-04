# drop_local_obsolete_slots

## Location
[src/backend/replication/logical/slotsync.c:417-473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L417-L473)

## Overview
Removes local synchronized replication slots that are no longer needed, either because they don't exist on the primary or are invalidated locally while still valid on the primary.

## Definition
```c
static void drop_local_obsolete_slots(List *remote_slot_list)
```

## Detailed Description
This function performs cleanup of synchronized replication slots by identifying and dropping slots that are obsolete. It handles several scenarios:

1. **Slots removed from primary**: Local slots that no longer exist in the remote slot list
2. **Failover-disabled slots**: Slots that are no longer enabled for failover on the primary
3. **Locally invalidated slots**: Slots that became invalid on the standby while remaining valid on the primary

The function implements careful synchronization to avoid race conditions with concurrent database drops and slot operations. It uses database-level shared locks and validates slot state before performing the actual drop operation.

Slots that are dropped due to local invalidation will be recreated in the next synchronization cycle, which is acceptable since these slots are not currently consumable on standby servers.

## Parameters / Member Variables
- `*remote_slot_list`: List of RemoteSlot structures representing the current slots from the primary server
## Dependencies
- Functions called/Symbols referenced:
  - [get_local_synced_slots](../g/get_local_synced_slots.md)
  - [local_sync_slot_required](../l/local_sync_slot_required.md)
  - [LockSharedObject](../L/LockSharedObject.md)
  - [UnlockSharedObject](../U/UnlockSharedObject.md)
  - [ReplicationSlotAcquire](../R/ReplicationSlotAcquire.md)
  - [ReplicationSlotDropAcquired](../R/ReplicationSlotDropAcquired.md)
  - foreach_ptr (macro)
  - SpinLockAcquire
  - SpinLockRelease
- Called from:
  - SLOTSYNC_COLUMN_COUNT (referenced by)

## Notes and Other Information
- Uses AccessShareLock on databases to prevent conflicts with ReplicationSlotsDropDBSlots()
- Implements a double-check pattern to verify slot state after acquiring database lock
- Logs dropped slots at LOG level for administrative visibility
- Handles edge cases like parallel database drops and slot recreations
- The function is safe to call concurrently due to proper locking mechanisms
- Dropped slots will be automatically recreated if they reappear on the primary in subsequent sync cycles

## Simplified Source

```c
static void
drop_local_obsolete_slots(List *remote_slot_list)
{
	// Get list of all local synced slots
	List *local_slots = get_local_synced_slots();

	// Check each local slot to see if it should be dropped
	foreach_ptr(ReplicationSlot, local_slot, local_slots) {
		// Skip slots that are still required
		if (!local_sync_slot_required(local_slot, remote_slot_list)) {
			bool synced_slot;

			// Lock database to prevent conflicts with concurrent drops
			LockSharedObject(DatabaseRelationId, local_slot->data.database,
							 0, AccessShareLock);

			// Double-check that slot is still synced after locking
			SpinLockAcquire(&local_slot->mutex);
			synced_slot = local_slot->in_use && local_slot->data.synced;
			SpinLockRelease(&local_slot->mutex);

			// Drop the slot if it's still a synced slot
			if (synced_slot) {
				ReplicationSlotAcquire(NameStr(local_slot->data.name), true);
				ReplicationSlotDropAcquired();
			}

			// Release database lock
			UnlockSharedObject(DatabaseRelationId, local_slot->data.database,
							   0, AccessShareLock);

			// Log the dropped slot
			ereport(LOG,
					errmsg("dropped replication slot \"%s\" of database with OID %u",
						   NameStr(local_slot->data.name),
						   local_slot->data.database));
		}
	}
}
```