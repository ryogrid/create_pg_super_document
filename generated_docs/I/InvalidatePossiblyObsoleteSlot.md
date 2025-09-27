# InvalidatePossiblyObsoleteSlot

## Location
[src/backend/replication/slot.c:1543-1774](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1543-L1774)

## Overview
Helper function that attempts to acquire and invalidate a specific replication slot if it meets the obsolescence criteria, handling active slot owners through process termination.

## Definition
static bool InvalidatePossiblyObsoleteSlot(ReplicationSlotInvalidationCause cause, ReplicationSlot *s, XLogRecPtr oldestLSN, Oid dboid, TransactionId snapshotConflictHorizon, bool *invalidated)

## Detailed Description
This complex function implements the core logic for invalidating potentially obsolete replication slots. It operates in a loop to handle race conditions and active slot ownership:

1. **Slot Assessment**: Checks if the slot meets invalidation criteria based on the cause:
   - RS_INVAL_WAL_REMOVED: Slot's restart_lsn is older than available WAL
   - RS_INVAL_HORIZON: Logical slot has conflicting transaction horizons for specific databases
   - RS_INVAL_WAL_LEVEL: Logical slot on standby with insufficient wal_level

2. **Acquisition Handling**: If the slot is unowned, immediately acquires and invalidates it. If owned by another process, signals the owner for termination and waits.

3. **Process Termination**: Sends appropriate signals (SIGTERM or PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT) to slot owners, with different handling for startup processes vs regular backends.

4. **State Persistence**: For successfully invalidated slots, ensures the invalidated state is persisted to disk via ReplicationSlotSave().

The function is inherently racy due to lock releases for syscalls, requiring careful coordination with the caller's restart logic.

## Parameters / Member Variables
- cause: Specific invalidation reason to check against
- s: Pointer to the replication slot to potentially invalidate  
- oldestLSN: Oldest available LSN for WAL removal checks
- dboid: Database OID for horizon conflicts (InvalidOid for shared relations)
- snapshotConflictHorizon: Transaction ID horizon for snapshot conflicts
- invalidated: Output parameter set to true if slot was successfully invalidated

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md)/LWLockRelease/LWLockAcquire
  - SlotIsLogical
  - TransactionIdIsValid/TransactionIdPrecedesOrEquals
  - [ConditionVariablePrepareToSleep](../C/ConditionVariablePrepareToSleep.md)/ConditionVariableSleep
  - [ReportSlotInvalidation](../R/ReportSlotInvalidation.md)
  - [SendProcSignal](../S/SendProcSignal.md)
  - kill
  - [ReplicationSlotMarkDirty](../R/ReplicationSlotMarkDirty.md)/ReplicationSlotSave/ReplicationSlotRelease
  - pg_unreachable
- Called from (representative examples):
  - [InvalidateObsoleteReplicationSlots](InvalidateObsoleteReplicationSlots.md)

## Notes and Other Information
This function implements PostgreSQL's slot invalidation strategy, balancing the need to free resources with minimizing disruption to active replication. The retry mechanism and careful lock management prevent race conditions while the process termination logic ensures that obsolete slots don't indefinitely block resource cleanup. The function's return value indicates whether locks were released, signaling the caller to restart its iteration.

## Simplified Source

```c
// Simplified version of InvalidatePossiblyObsoleteSlot
static bool InvalidatePossiblyObsoleteSlot(ReplicationSlotInvalidationCause cause,
                                         ReplicationSlot *s, XLogRecPtr oldestLSN,
                                         Oid dboid, TransactionId snapshotConflictHorizon,
                                         bool *invalidated) {
    int last_signaled_pid = 0;
    bool released_lock = false;
    bool terminated = false;

    // Record initial slot state for consistent invalidation checks
    TransactionId initial_effective_xmin = InvalidTransactionId;
    TransactionId initial_catalog_effective_xmin = InvalidTransactionId;
    XLogRecPtr initial_restart_lsn = InvalidXLogRecPtr;

    // Main processing loop - continues until slot processed or abandoned
    for (;;) {
        Assert(LWLockHeldByMeInMode(ReplicationSlotControlLock, LW_SHARED));

        // Early exit if slot is no longer in use
        if (!s->in_use) {
            cleanup_and_exit(released_lock);
            break;
        }

        // Step 1: Check if slot should be invalidated
        SpinLockAcquire(&s->mutex);

        XLogRecPtr restart_lsn = s->data.restart_lsn;
        NameData slotname = s->data.name;
        int active_pid = s->active_pid;
        ReplicationSlotInvalidationCause invalidation_cause = RS_INVAL_NONE;

        // Skip if already invalidated
        if (s->data.invalidated == RS_INVAL_NONE) {
            // Record initial state for consistent checking
            if (!terminated) {
                capture_initial_slot_state(s, &initial_restart_lsn,
                                         &initial_effective_xmin,
                                         &initial_catalog_effective_xmin);
            }

            // Step 2: Determine if slot meets invalidation criteria
            invalidation_cause = check_invalidation_criteria(cause, s, oldestLSN,
                                                           dboid, snapshotConflictHorizon,
                                                           initial_restart_lsn,
                                                           initial_effective_xmin,
                                                           initial_catalog_effective_xmin);
        }

        // Step 3: Handle slot based on invalidation result
        if (invalidation_cause == RS_INVAL_NONE) {
            SpinLockRelease(&s->mutex);
            cleanup_and_exit(released_lock);
            break;
        }

        // Step 4: Attempt to acquire and invalidate the slot
        if (active_pid == 0) {
            // Slot is available - acquire and invalidate immediately
            acquire_and_invalidate_slot(s, invalidation_cause, invalidated);
            SpinLockRelease(&s->mutex);

            // Persist invalidation and report
            persist_invalidation_and_report(s, invalidation_cause, slotname,
                                          restart_lsn, oldestLSN, snapshotConflictHorizon);
            released_lock = true;
            break;
        } else {
            // Step 5: Slot is active - signal owner and wait
            SpinLockRelease(&s->mutex);

            released_lock = handle_active_slot(s, active_pid, invalidation_cause,
                                             &last_signaled_pid, &terminated,
                                             slotname, restart_lsn, oldestLSN,
                                             snapshotConflictHorizon);

            // Reacquire lock and continue loop
            LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
            continue;
        }
    }

    Assert(released_lock == !LWLockHeldByMe(ReplicationSlotControlLock));
    return released_lock;
}

// Helper function (conceptual)
static ReplicationSlotInvalidationCause check_invalidation_criteria(
    ReplicationSlotInvalidationCause cause, ReplicationSlot *s,
    XLogRecPtr oldestLSN, Oid dboid, TransactionId snapshotConflictHorizon,
    XLogRecPtr initial_restart_lsn, TransactionId initial_effective_xmin,
    TransactionId initial_catalog_effective_xmin) {

    switch (cause) {
        case RS_INVAL_WAL_REMOVED:
            if (initial_restart_lsn != InvalidXLogRecPtr && initial_restart_lsn < oldestLSN)
                return cause;
            break;

        case RS_INVAL_HORIZON:
            if (SlotIsLogical(s) &&
                (dboid == InvalidOid || dboid == s->data.database)) {
                if (xmin_conflicts_with_horizon(initial_effective_xmin, snapshotConflictHorizon) ||
                    xmin_conflicts_with_horizon(initial_catalog_effective_xmin, snapshotConflictHorizon))
                    return cause;
            }
            break;

        case RS_INVAL_WAL_LEVEL:
            if (SlotIsLogical(s))
                return cause;
            break;

        case RS_INVAL_NONE:
            pg_unreachable();
    }

    return RS_INVAL_NONE;
}
```

Key simplifications made:
- Organized into clear sequential steps with descriptive comments
- Abstracted complex conditional logic into conceptual helper functions
- Simplified the slot state capture and invalidation criteria checking
- Maintained the essential retry loop and race condition handling
- Preserved the critical process signaling and waiting logic
- Focused on the core algorithm: check criteria, acquire if possible, signal if needed
- Simplified while preserving the function's complex concurrent behavior