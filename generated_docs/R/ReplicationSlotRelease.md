# ReplicationSlotRelease

## Location
[src/backend/replication/slot.c:652-744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L652-L744)

## Overview
Releases the replication slot that the current backend considers to own, allowing this or another backend to re-acquire the slot later while preserving the slot's required resources.

## Definition
void ReplicationSlotRelease(void)

## Detailed Description
ReplicationSlotRelease is responsible for cleanly releasing a replication slot that is currently held by the calling backend. The function handles different types of slots (ephemeral vs persistent, logical vs physical) appropriately:

- For ephemeral slots, it completely drops the slot since ephemeral slots are meant to be temporary
- For persistent slots, it marks them as inactive while preserving their state for future use
- It properly manages transaction ID constraints and timing information
- Updates process-level flags to indicate the backend is no longer performing logical decoding
- Provides appropriate logging for WAL sender processes

The function ensures that slot resources are properly cleaned up and that other processes waiting for the slot are notified of its availability.

## Parameters / Member Variables
This function takes no parameters but operates on the global MyReplicationSlot variable.

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotDropAcquired](ReplicationSlotDropAcquired.md)
  - SlotIsLogical  
  - [ReplicationSlotsComputeRequiredXmin](ReplicationSlotsComputeRequiredXmin.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [ConditionVariableBroadcast](../C/ConditionVariableBroadcast.md)
- Called from (representative examples):
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)
  - [synchronize_one_slot](../s/synchronize_one_slot.md)
  - [ReplicationSlotShmemExit](ReplicationSlotShmemExit.md)
  - [WalSndErrorCleanup](../W/WalSndErrorCleanup.md)
  - [PostgresMain](../P/PostgresMain.md)

## Notes and Other Information
- The function handles both logical and physical replication slots
- Ephemeral slots are immediately dropped while persistent slots are marked inactive
- The function manages effective_xmin constraints for catalog snapshot creation
- Process status flags are updated to reflect that logical decoding has stopped
- Appropriate logging is performed for WAL sender processes
- The function is designed to be safe even in error conditions where cleanup may be incomplete

## Simplified Source

```c
// Simplified version of ReplicationSlotRelease
void ReplicationSlotRelease(void) {
    ReplicationSlot *slot = MyReplicationSlot;
    char *slotname = NULL;
    bool is_logical = false;
    TimestampTz now = 0;

    // Basic validation
    Assert(slot != NULL && slot->active_pid != 0);

    // Save slot info for logging if this is a WAL sender
    if (am_walsender) {
        slotname = pstrdup(NameStr(slot->data.name));
        is_logical = SlotIsLogical(slot);
    }

    // Core logic step 1: Handle ephemeral slots by dropping them completely
    if (slot->data.persistency == RS_EPHEMERAL) {
        ReplicationSlotDropAcquired();
    }

    // Core logic step 2: Remove temporary transaction ID constraints
    if (!TransactionIdIsValid(slot->data.xmin) &&
        TransactionIdIsValid(slot->effective_xmin)) {
        SpinLockAcquire(&slot->mutex);
        slot->effective_xmin = InvalidTransactionId;
        SpinLockRelease(&slot->mutex);
        ReplicationSlotsComputeRequiredXmin(false);
    }

    // Core logic step 3: Mark slot as inactive with timestamp
    now = GetCurrentTimestamp();

    if (slot->data.persistency == RS_PERSISTENT) {
        // Mark persistent slot inactive and notify waiters
        SpinLockAcquire(&slot->mutex);
        slot->active_pid = 0;
        slot->inactive_since = now;
        SpinLockRelease(&slot->mutex);
        ConditionVariableBroadcast(&slot->active_cv);
    } else {
        // Just set inactive time for non-persistent slots
        SpinLockAcquire(&slot->mutex);
        slot->inactive_since = now;
        SpinLockRelease(&slot->mutex);
    }

    // Core logic step 4: Clear global slot reference
    MyReplicationSlot = NULL;

    // Core logic step 5: Update process status flags
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    MyProc->statusFlags &= ~PROC_IN_LOGICAL_DECODING;
    ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;
    LWLockRelease(ProcArrayLock);

    // Core logic step 6: Log the release for WAL senders
    if (am_walsender) {
        ereport(log_replication_commands ? LOG : DEBUG1,
                is_logical
                ? errmsg("released logical replication slot \"%s\"", slotname)
                : errmsg("released physical replication slot \"%s\"", slotname));
        pfree(slotname);
    }
}
```

Key simplifications made:
- Removed redundant variable initializations and comments for compiler quieting
- Consolidated the slot type handling logic flow
- Abstracted low-level spinlock operations with clear descriptions
- Focused on the main execution path and core functionality
- Added step-by-step comments for each major operation
- Maintained the essential algorithm while improving readability