# ReplicationSlotAlter

## Location
[src/backend/replication/slot.c:807-867](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L807-L867)

## Overview
Changes the definition of a replication slot identified by name, specifically allowing modification of the failover property with comprehensive validation checks.

## Definition
void ReplicationSlotAlter(const char *name, bool failover)

## Detailed Description
ReplicationSlotAlter provides functionality to modify the configuration of an existing logical replication slot, currently supporting changes to the failover property. The function performs extensive validation:

1. Ensures only logical replication slots can be altered (physical slots are not supported)
2. Prevents alteration of synced slots during recovery to maintain synchronization integrity  
3. Disallows enabling failover on standby servers since cascading standby synchronization is not supported
4. Prevents enabling failover for temporary slots as they are not synchronized to standbys
5. Only persists changes if the failover value actually differs from the current setting

The function follows the standard pattern of acquiring the slot, making validated modifications, persisting the changes, and releasing the slot.

## Parameters / Member Variables
- : The name of the replication slot to alter
- : Boolean flag indicating whether to enable or disable failover functionality for the slot

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotAcquire](ReplicationSlotAcquire.md)
  - SlotIsPhysical
  - [RecoveryInProgress](RecoveryInProgress.md)
  - [ReplicationSlotMarkDirty](ReplicationSlotMarkDirty.md)
  - [ReplicationSlotSave](ReplicationSlotSave.md)
  - [ReplicationSlotRelease](ReplicationSlotRelease.md)
  - RS_TEMPORARY (slot persistency constant)
- Called from (representative examples):
  - [AlterReplicationSlot](../A/AlterReplicationSlot.md)

## Notes and Other Information
- Only supports logical replication slots - physical slots will generate an error
- Includes comprehensive validation for different deployment scenarios (primary/standby)
- Changes are only persisted if the new failover value differs from the current value
- Temporary slots cannot have failover enabled due to synchronization limitations
- Synced slots cannot be altered during recovery to maintain consistency
- The function properly handles slot acquisition and release for safe concurrent access
- Used primarily by the ALTER REPLICATION SLOT SQL command through the replication protocol

## Simplified Source

```c
// Simplified version of ReplicationSlotAlter
void ReplicationSlotAlter(const char *name, bool failover) {
    Assert(MyReplicationSlot == NULL);

    // Acquire the replication slot
    ReplicationSlotAcquire(name, false);

    // Validate slot type - only logical slots supported
    if (SlotIsPhysical(MyReplicationSlot)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("cannot use %s with a physical replication slot",
                             "ALTER_REPLICATION_SLOT")));
    }

    // Handle recovery mode restrictions
    if (RecoveryInProgress()) {
        // Don't allow altering synced slots
        if (MyReplicationSlot->data.synced) {
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                          errmsg("cannot alter replication slot \"%s\"", name),
                          errdetail("This replication slot is being synchronized from the primary server.")));
        }

        // Don't allow enabling failover on standby
        if (failover) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                          errmsg("cannot enable failover for a replication slot on the standby")));
        }
    }

    // Don't allow failover for temporary slots
    if (failover && MyReplicationSlot->data.persistency == RS_TEMPORARY) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("cannot enable failover for a temporary replication slot")));
    }

    // Update failover setting if it has changed
    if (MyReplicationSlot->data.failover != failover) {
        SpinLockAcquire(&MyReplicationSlot->mutex);
        MyReplicationSlot->data.failover = failover;
        SpinLockRelease(&MyReplicationSlot->mutex);

        // Mark dirty and save the changes
        ReplicationSlotMarkDirty();
        ReplicationSlotSave();
    }

    // Release the slot
    ReplicationSlotRelease();
}
```

Key simplifications made:
- Added clear comments for each validation step
- Preserved all essential error checking and slot management
- Maintained proper locking around slot data modification
- Kept the atomic update pattern for slot persistence