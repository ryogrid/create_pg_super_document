# StandbySlotsHaveCaughtup

## Location
[src/backend/replication/slot.c:2592-2745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L2592-L2745)

## Overview
StandbySlotsHaveCaughtup checks whether all standby replication slots specified in the synchronized_standby_slots configuration have caught up to a given WAL location, returning true if all specified slots have progressed beyond the target position.

## Definition
```c
bool StandbySlotsHaveCaughtup(XLogRecPtr wait_for_lsn, int elevel)
```

## Detailed Description
This function validates that all physical replication slots specified in the synchronized_standby_slots configuration parameter have caught up to or beyond a specified WAL (Write-Ahead Log) location. It is primarily used to ensure data consistency in logical replication scenarios where the primary database needs to wait for standby servers to process up to a certain point before proceeding.

The function performs comprehensive validation of each configured slot, checking for existence, validity, activity status, and actual progress. It maintains the ss_oldest_flush_lsn global variable to track the minimum restart LSN across all synchronized slots for performance optimization in subsequent calls.

Early returns occur when: no synchronized slots are configured, the server is in recovery mode (standbys don't sync to cascading standbys), or the cached ss_oldest_flush_lsn already indicates all slots have caught up.

## Parameters / Member Variables
- `wait_for_lsn`: The target WAL location (XLogRecPtr) that all synchronized standby slots must reach or exceed
- `elevel`: Error level for logging messages when slots don't exist, are invalidated, or are inactive (e.g., WARNING, ERROR)

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - XLogRecPtrIsInvalid
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (with ReplicationSlotControlLock)
  - [SearchNamedReplicationSlot](SearchNamedReplicationSlot.md)
  - SlotIsLogical
  - ereport
- Called from (representative examples):
  - [WaitForStandbyConfirmation](../W/WaitForStandbyConfirmation.md)
  - [NeedToWaitForStandbys](../N/NeedToWaitForStandbys.md)

## Notes and Other Information
- Returns true immediately if synchronized_standby_slots is not configured or if running on a standby server
- Uses ReplicationSlotControlLock in shared mode to prevent concurrent slot operations during validation
- Updates the global ss_oldest_flush_lsn variable with the minimum restart LSN of all valid slots for caching purposes
- Validates that all specified slots are physical (not logical) replication slots
- Provides detailed error messages with hints for common configuration issues like missing or invalidated slots
- The function is critical for maintaining consistency in logical replication scenarios where coordination between primary and standby servers is required

## Simplified Source

```c
bool
StandbySlotsHaveCaughtup(XLogRecPtr wait_for_lsn, int elevel)
{
    int caught_up_slots = 0;
    XLogRecPtr min_restart_lsn = InvalidXLogRecPtr;

    // Early returns for common cases
    if (synchronized_standby_slots_config == NULL)
        return true;

    if (RecoveryInProgress())
        return true;

    if (!XLogRecPtrIsInvalid(ss_oldest_flush_lsn) &&
        ss_oldest_flush_lsn >= wait_for_lsn)
        return true;

    // Lock to prevent concurrent slot changes
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    // Check each configured standby slot
    const char *name = synchronized_standby_slots_config->slot_names;
    for (int i = 0; i < synchronized_standby_slots_config->nslotnames; i++) {
        ReplicationSlot *slot = SearchNamedReplicationSlot(name, false);

        // Validate slot exists and is physical
        if (!slot) {
            ereport(elevel, (errmsg("replication slot \"%s\" does not exist", name)));
            break;
        }

        if (SlotIsLogical(slot)) {
            ereport(elevel, (errmsg("cannot specify logical slot \"%s\"", name)));
            break;
        }

        // Check slot status
        SpinLockAcquire(&slot->mutex);
        XLogRecPtr restart_lsn = slot->data.restart_lsn;
        bool invalidated = slot->data.invalidated != RS_INVAL_NONE;
        bool inactive = slot->active_pid == 0;
        SpinLockRelease(&slot->mutex);

        // Handle invalidated or behind slots
        if (invalidated) {
            ereport(elevel, (errmsg("slot \"%s\" has been invalidated", name)));
            break;
        }

        if (XLogRecPtrIsInvalid(restart_lsn) || restart_lsn < wait_for_lsn) {
            if (inactive)
                ereport(elevel, (errmsg("slot \"%s\" is not active", name)));
            break;
        }

        // Track minimum restart LSN
        if (XLogRecPtrIsInvalid(min_restart_lsn) || min_restart_lsn > restart_lsn)
            min_restart_lsn = restart_lsn;

        caught_up_slots++;
        name += strlen(name) + 1;
    }

    LWLockRelease(ReplicationSlotControlLock);

    // All slots must have caught up
    if (caught_up_slots != synchronized_standby_slots_config->nslotnames)
        return false;

    // Update cached minimum flush LSN
    ss_oldest_flush_lsn = min_restart_lsn;
    return true;
}
```