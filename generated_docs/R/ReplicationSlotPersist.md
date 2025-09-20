# ReplicationSlotPersist

## Location
[src/backend/replication/slot.c:1027-1048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1027-L1048)

## Overview
Converts an ephemeral or temporary replication slot to a persistent slot, ensuring it survives server crashes.

## Definition

```c
void
ReplicationSlotPersist(void)
```
## Detailed Description
This function performs the critical operation of upgrading a replication slot from ephemeral (RS_EPHEMERAL) or temporary (RS_TEMPORARY) status to persistent (RS_PERSISTENT) status. Once converted, the slot will be automatically restored after server restarts, crashes, or other interruptions, ensuring continuity of replication.

The function performs an atomic state change by acquiring the slot's spinlock, updating the persistency flag, and then immediately marking the slot as dirty and saving it to disk. This ensures that the persistence change takes effect immediately and is durable.

This operation is particularly important during logical replication setup, where slots are often created as temporary during initial configuration and then converted to persistent once the replication relationship is established and confirmed to be working correctly.

## Parameters / Member Variables
- No parameters (operates on the global MyReplicationSlot variable)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - [ReplicationSlotMarkDirty](ReplicationSlotMarkDirty.md)
  - [ReplicationSlotSave](ReplicationSlotSave.md)
  - RS_PERSISTENT (enum value)
  - [ReplicationSlot](ReplicationSlot.md) (struct type)
- Called from (representative examples):
  - [pg_create_logical_replication_slot](../p/pg_create_logical_replication_slot.md)
  - [copy_replication_slot](../c/copy_replication_slot.md)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)
  - [update_and_persist_local_synced_slot](../u/update_and_persist_local_synced_slot.md)

## Notes and Other Information
- Requires that MyReplicationSlot is not NULL (verified by Assert)
- Requires that the slot is currently NOT already persistent (verified by Assert)
- Immediately saves the slot to disk after changing persistence flag
- Uses spinlock for thread-safe modification of slot metadata
- Essential for logical replication setup where slots start as temporary
- Once a slot becomes persistent, it cannot be reverted to ephemeral status
- The slot will appear in pg_replication_slots view after this operation
- Critical for ensuring replication continuity across server restarts