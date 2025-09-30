# ReplicationSlotsDropDBSlots

## Location
[src/backend/replication/slot.c:1270-1361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1270-L1361)

## Overview
Drops all database-specific logical replication slots associated with a given database OID, typically called during database drop operations.

## Definition
```c
void ReplicationSlotsDropDBSlots(Oid dboid)
```

## Detailed Description
This function iterates through all replication slots to find and drop logical slots that are associated with a specific database. It uses a restart-based approach where the scan is restarted from the beginning each time a slot is dropped, since releasing the control lock to perform filesystem operations can change the slot set.

The function expects the caller to hold an exclusive lock on the pg_database entry for the target database to prevent creation of new slots or replay from existing slots. However, it can still encounter active slots from concurrent sessions (e.g., a backend dropping a slot while connected to another database) and will error in such cases.

The function intentionally includes invalidated slots in the drop operation since they still represent references to the database. It temporarily acquires each target slot before dropping it using ReplicationSlotDropAcquired().

## Parameters / Member Variables
- `dboid`: OID of the database whose replication slots should be dropped

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (with LW_SHARED mode)
  - [LWLockRelease](../L/LWLockRelease.md)
  - SpinLockAcquire
  - SpinLockRelease
  - SlotIsLogical
  - [ReplicationSlotDropAcquired](ReplicationSlotDropAcquired.md)
  - [ReplicationSlot](ReplicationSlot.md) (struct access)
  - NameStr (macro)
  - ereport/ERROR

- Called from (representative examples):
  - [dropdb](../d/dropdb.md) (database drop command)
  - [dbase_redo](../d/dbase_redo.md) (WAL replay during database operations)

## Notes and Other Information
- Uses a restart-based scan pattern due to releasing locks during slot drops
- Errors if it encounters active slots (active_pid != 0) to avoid conflicts
- Includes detailed comments about potential race conditions with slot sync workers
- Not optimized for efficiency since database drops with many slots are rare
- Only processes logical slots since physical slots are not database-specific
- Temporarily sets MyReplicationSlot and active_pid to acquire the slot before dropping
- The restart approach ensures consistency despite lock releases for filesystem operations

## Simplified Source

```c
void ReplicationSlotsDropDBSlots(Oid dboid) {
    int i;

    if (max_replication_slots <= 0)
        return;

restart:
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (i = 0; i < max_replication_slots; i++) {
        ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];
        char *slotname;
        int active_pid;

        // Skip unused slots
        if (!s->in_use)
            continue;

        // Only logical slots are database-specific
        if (!SlotIsLogical(s))
            continue;

        // Skip slots for other databases
        if (s->data.database != dboid)
            continue;

        // Acquire slot information
        SpinLockAcquire(&s->mutex);
        slotname = NameStr(s->data.name);
        active_pid = s->active_pid;
        if (active_pid == 0) {
            // Acquire the slot for dropping
            MyReplicationSlot = s;
            s->active_pid = MyProcPid;
        }
        SpinLockRelease(&s->mutex);

        // Error if slot is active in another session
        if (active_pid) {
            ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
                           errmsg("replication slot \"%s\" is active for PID %d",
                                  slotname, active_pid)));
        }

        // Drop the acquired slot and restart scan
        LWLockRelease(ReplicationSlotControlLock);
        ReplicationSlotDropAcquired();
        goto restart;
    }
    LWLockRelease(ReplicationSlotControlLock);
}
```