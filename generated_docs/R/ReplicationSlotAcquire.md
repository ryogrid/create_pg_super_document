# ReplicationSlotAcquire

## Location
[src/backend/replication/slot.c:540-651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L540-L651)

## Overview
Finds and acquires an existing replication slot by name, marking it as active for the current process.

## Definition

```c
void
ReplicationSlotAcquire(const char *name, bool nowait)
```
## Detailed Description
ReplicationSlotAcquire locates a replication slot by name and attempts to acquire it for the current process. The function implements both blocking and non-blocking acquisition modes based on the nowait parameter. When nowait is false, the function will wait indefinitely for the slot to become available if it's currently in use by another process. When nowait is true, it immediately errors if the slot is active.

The function uses a combination of lightweight locks and condition variables to coordinate slot access between processes. It employs a retry mechanism for the blocking case, using condition variables to sleep until the owning process releases the slot. Upon successful acquisition, it sets up statistics tracking for logical slots and logs the acquisition for WAL senders.

## Parameters / Member Variables
- `*name`: The name of the replication slot to acquire (must not be NULL)
- `nowait`: If true, error immediately if slot is in use; if false, wait for slot to become available
## Dependencies
- Functions called/Symbols referenced:
  - [SearchNamedReplicationSlot](../S/SearchNamedReplicationSlot.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [ConditionVariablePrepareToSleep](../C/ConditionVariablePrepareToSleep.md)/ConditionVariableSleep/ConditionVariableCancelSleep/ConditionVariableBroadcast
  - SpinLockAcquire/SpinLockRelease
  - SlotIsLogical
  - [pgstat_acquire_replslot](../p/pgstat_acquire_replslot.md)
- Called from (representative examples):
  - [StartReplication](../S/StartReplication.md)
  - [StartLogicalReplication](../S/StartLogicalReplication.md)
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)
  - [synchronize_one_slot](../s/synchronize_one_slot.md)

## Notes and Other Information
- Sets MyReplicationSlot global variable upon successful acquisition
- Uses retry loop with condition variables for blocking acquisition mode
- Resets the slot's inactive_since timestamp when acquired
- Provides different error messages for non-existent vs. in-use slots
- Logs acquisition events for WAL sender processes based on log_replication_commands setting
- Handles both single-user mode (no concurrency checks) and multi-user mode
- Protects against stale statistics from previous slot usage by calling pgstat_acquire_replslot for logical slots

## Simplified Source

```c
// Simplified version of ReplicationSlotAcquire
void ReplicationSlotAcquire(const char *name, bool nowait) {
    ReplicationSlot *s;
    int active_pid;

    Assert(name != NULL);

retry:
    Assert(MyReplicationSlot == NULL);

    // Find the named replication slot
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    s = SearchNamedReplicationSlot(name, false);

    if (s == NULL || !s->in_use) {
        LWLockRelease(ReplicationSlotControlLock);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                       errmsg("replication slot \"%s\" does not exist", name)));
    }

    // Check if slot is active in another process
    if (IsUnderPostmaster) {
        // Prepare to wait if needed
        if (!nowait)
            ConditionVariablePrepareToSleep(&s->active_cv);

        // Atomically check and claim the slot
        SpinLockAcquire(&s->mutex);
        if (s->active_pid == 0)
            s->active_pid = MyProcPid;
        active_pid = s->active_pid;
        SpinLockRelease(&s->mutex);
    } else {
        // Single user mode - no concurrency
        s->active_pid = active_pid = MyProcPid;
    }

    LWLockRelease(ReplicationSlotControlLock);

    // Handle slot already in use by another process
    if (active_pid != MyProcPid) {
        if (!nowait) {
            // Wait for slot to be released and retry
            ConditionVariableSleep(&s->active_cv, WAIT_EVENT_REPLICATION_SLOT_DROP);
            ConditionVariableCancelSleep();
            goto retry;
        }

        ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
                       errmsg("replication slot \"%s\" is active for PID %d",
                             NameStr(s->data.name), active_pid)));
    } else if (!nowait) {
        ConditionVariableCancelSleep(); // no sleep needed
    }

    // Successfully acquired the slot
    ConditionVariableBroadcast(&s->active_cv);
    MyReplicationSlot = s;

    // Initialize statistics for logical slots
    if (SlotIsLogical(s))
        pgstat_acquire_replslot(s);

    // Reset inactive timer
    SpinLockAcquire(&s->mutex);
    s->inactive_since = 0;
    SpinLockRelease(&s->mutex);

    // Log acquisition for WAL senders
    if (am_walsender) {
        ereport(log_replication_commands ? LOG : DEBUG1,
                SlotIsLogical(s)
                ? errmsg("acquired logical replication slot \"%s\"", NameStr(s->data.name))
                : errmsg("acquired physical replication slot \"%s\"", NameStr(s->data.name)));
    }
}
```

Key simplifications made:
- Added clear comments explaining each major step
- Grouped related operations together logically
- Simplified error handling while preserving essential checks
- Maintained the retry mechanism and concurrency control
- Preserved all critical locking and synchronization logic