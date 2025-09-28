# ReplicationSlotCreate

## Location
[src/backend/replication/slot.c:309-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L309-L463)

## Overview
Creates a new replication slot and marks it as used by the current backend, handling both physical and logical replication slots with various configuration options.

## Definition

```c
void
ReplicationSlotCreate(const char *name, bool db_specific,
					  ReplicationSlotPersistency persistency,
					  bool two_phase, bool failover, bool synced)
```
## Detailed Description
ReplicationSlotCreate is responsible for creating and initializing a new replication slot in PostgreSQL. It performs comprehensive validation, handles resource allocation, and ensures thread-safe creation with proper locking mechanisms. The function supports various replication slot types including physical slots for streaming replication and logical slots for logical decoding, with advanced features like two-phase commit support and failover capabilities.

The function implements strict validation rules to prevent invalid configurations, such as disallowing failover-enabled slots on standby servers (except during slot synchronization) and preventing temporary failover slots. It uses a combination of ReplicationSlotAllocationLock and ReplicationSlotControlLock to ensure atomic slot creation and prevent race conditions.

## Parameters / Member Variables
- : The name of the replication slot to create (must be unique)
- : If true, the slot is database-specific for logical decoding; if false, it's for physical replication
- : Determines slot persistence (RS_PERSISTENT, RS_EPHEMERAL, or RS_TEMPORARY)
- : Enables decoding of prepared transactions for logical slots (can only be set at creation time)
- : Enables slot synchronization to standbys for logical replication continuity after failover
- : Indicates if the slot is synchronized from a primary server

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotValidateName](ReplicationSlotValidateName.md)
  - [RecoveryInProgress](RecoveryInProgress.md)
  - [IsSyncingReplicationSlots](../I/IsSyncingReplicationSlots.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [CreateSlotOnDisk](../C/CreateSlotOnDisk.md)
  - [pgstat_create_replslot](../p/pgstat_create_replslot.md)
  - [ConditionVariableBroadcast](../C/ConditionVariableBroadcast.md)
- Called from (representative examples):
  - [create_physical_replication_slot](../c/create_physical_replication_slot.md)
  - [create_logical_replication_slot](../c/create_logical_replication_slot.md)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (walsender)
  - [synchronize_one_slot](../s/synchronize_one_slot.md)

## Notes and Other Information
- The function ensures atomic slot creation using exclusive locking on ReplicationSlotAllocationLock
- Validates slot name uniqueness and enforces max_replication_slots limit
- Sets MyReplicationSlot global variable to the newly created slot
- Creates statistics entries only for logical slots
- Two-phase commit option cannot be changed after slot creation to maintain consistency
- Failover slots cannot be created on standby servers except during slot synchronization process
- The slot is marked as active with the current process PID upon successful creation

## Simplified Source

```c
// Simplified version of ReplicationSlotCreate
void ReplicationSlotCreate(const char *name, bool db_specific,
                          ReplicationSlotPersistency persistency,
                          bool two_phase, bool failover, bool synced) {
    ReplicationSlot *slot = NULL;
    int i;

    Assert(MyReplicationSlot == NULL);

    // Validate slot name
    ReplicationSlotValidateName(name, ERROR);

    // Validate failover constraints
    if (failover) {
        if (RecoveryInProgress() && !IsSyncingReplicationSlots()) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                          errmsg("cannot enable failover for a replication slot created on the standby")));
        }
        if (persistency == RS_TEMPORARY && !IsSyncingReplicationSlots()) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                          errmsg("cannot enable failover for a temporary replication slot")));
        }
    }

    // Acquire allocation lock to prevent concurrent creation
    LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);

    // Find available slot and check for name collision
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (i = 0; i < max_replication_slots; i++) {
        ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

        if (s->in_use && strcmp(name, NameStr(s->data.name)) == 0) {
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                          errmsg("replication slot \"%s\" already exists", name)));
        }
        if (!s->in_use && slot == NULL) {
            slot = s;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    // Check if all slots are in use
    if (slot == NULL) {
        ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                      errmsg("all replication slots are in use"),
                      errhint("Free one or increase \"max_replication_slots\".")));
    }

    // Initialize slot data
    Assert(!slot->in_use);
    Assert(slot->active_pid == 0);

    memset(&slot->data, 0, sizeof(ReplicationSlotPersistentData));
    namestrcpy(&slot->data.name, name);
    slot->data.database = db_specific ? MyDatabaseId : InvalidOid;
    slot->data.persistency = persistency;
    slot->data.two_phase = two_phase;
    slot->data.two_phase_at = InvalidXLogRecPtr;
    slot->data.failover = failover;
    slot->data.synced = synced;

    // Initialize shared memory fields
    slot->just_dirtied = false;
    slot->dirty = false;
    slot->effective_xmin = InvalidTransactionId;
    slot->effective_catalog_xmin = InvalidTransactionId;
    slot->candidate_catalog_xmin = InvalidTransactionId;
    slot->candidate_xmin_lsn = InvalidXLogRecPtr;
    slot->candidate_restart_valid = InvalidXLogRecPtr;
    slot->candidate_restart_lsn = InvalidXLogRecPtr;
    slot->last_saved_confirmed_flush = InvalidXLogRecPtr;
    slot->inactive_since = 0;

    // Create slot on disk
    CreateSlotOnDisk(slot);

    // Mark slot as in use and active
    LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE);
    slot->in_use = true;

    SpinLockAcquire(&slot->mutex);
    slot->active_pid = MyProcPid;
    SpinLockRelease(&slot->mutex);
    MyReplicationSlot = slot;

    LWLockRelease(ReplicationSlotControlLock);

    // Create statistics entry for logical slots
    if (SlotIsLogical(slot)) {
        pgstat_create_replslot(slot);
    }

    LWLockRelease(ReplicationSlotAllocationLock);

    // Notify other processes
    ConditionVariableBroadcast(&slot->active_cv);
}
```

Key simplifications made:
- Added clear comments for each major step
- Consolidated error handling
- Maintained all essential validation and locking
- Preserved atomic slot creation process
- Kept all initialization and safety checks