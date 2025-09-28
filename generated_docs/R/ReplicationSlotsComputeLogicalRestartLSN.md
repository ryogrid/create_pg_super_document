# ReplicationSlotsComputeLogicalRestartLSN

## Location
[src/backend/replication/slot.c:1154-1211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1154-L1211)

## Overview
Computes the oldest WAL LSN required by logical decoding slots, excluding physical replication slots from consideration.

## Definition
```c
XLogRecPtr ReplicationSlotsComputeLogicalRestartLSN(void)
```

## Detailed Description
This function iterates through all replication slots to find the oldest restart LSN among active, non-invalidated logical replication slots specifically. Unlike ReplicationSlotsComputeRequiredLSN which considers all slots, this function filters for logical slots only using SlotIsLogical().

The function returns InvalidXLogRecPtr if logical decoding is disabled (max_replication_slots <= 0), no logical slots exist, or no logical slots have valid restart LSNs. The results are computed on-demand rather than being maintained as a precomputed value, since they aren't required frequently.

The returned LSN value is guaranteed to be >= the value returned by ReplicationSlotsComputeRequiredLSN() since it ignores physical replication slots which might have older restart LSNs.

## Parameters / Member Variables
This function takes no parameters and returns:
- `XLogRecPtr`: The oldest restart LSN among logical slots, or InvalidXLogRecPtr if none found

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (with LW_SHARED mode)
  - [LWLockRelease](../L/LWLockRelease.md)
  - SpinLockAcquire
  - SpinLockRelease
  - SlotIsLogical
  - [ReplicationSlot](ReplicationSlot.md) (struct access)
  - RS_INVAL_NONE (invalidation state constant)

- Called from (representative examples):
  - [CheckPointLogicalRewriteHeap](../C/CheckPointLogicalRewriteHeap.md)
  - [CheckPointSnapBuild](../C/CheckPointSnapBuild.md)

## Notes and Other Information
- Specifically designed for logical decoding purposes, filtering out physical replication slots
- Results are computed on-demand rather than cached, unlike some other slot computation functions
- Early return with InvalidXLogRecPtr if max_replication_slots <= 0 (logical decoding disabled)
- Properly handles the case where restart_lsn might be InvalidXLogRecPtr for individual slots
- Uses the same locking strategy as ReplicationSlotsComputeRequiredLSN but with additional logical slot filtering

## Simplified Source

```c
// Simplified version of ReplicationSlotsComputeLogicalRestartLSN
XLogRecPtr ReplicationSlotsComputeLogicalRestartLSN(void) {
    XLogRecPtr oldest_lsn = InvalidXLogRecPtr;

    // Early return if logical decoding is disabled
    if (max_replication_slots <= 0)
        return InvalidXLogRecPtr;

    // Lock the replication slot control structure
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    // Iterate through all replication slots
    for (int i = 0; i < max_replication_slots; i++) {
        ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[i];

        // Skip unused slots
        if (!slot->in_use)
            continue;

        // Skip physical slots - we only want logical slots
        if (!SlotIsLogical(slot))
            continue;

        // Read slot data atomically
        SpinLockAcquire(&slot->mutex);
        XLogRecPtr restart_lsn = slot->data.restart_lsn;
        bool is_invalidated = (slot->data.invalidated != RS_INVAL_NONE);
        SpinLockRelease(&slot->mutex);

        // Skip invalidated slots and slots with no valid LSN
        if (is_invalidated || restart_lsn == InvalidXLogRecPtr)
            continue;

        // Keep track of the oldest (smallest) LSN
        if (oldest_lsn == InvalidXLogRecPtr || restart_lsn < oldest_lsn)
            oldest_lsn = restart_lsn;
    }

    LWLockRelease(ReplicationSlotControlLock);
    return oldest_lsn;
}
```

Key simplifications made:
- Renamed variables for clarity (result → oldest_lsn, s → slot, etc.)
- Added descriptive comments for each major step
- Simplified the conditional logic flow for better readability
- Combined variable declarations with assignments where appropriate
- Made the purpose of each filtering step more explicit
- Preserved all essential logic including locking, filtering, and LSN comparison