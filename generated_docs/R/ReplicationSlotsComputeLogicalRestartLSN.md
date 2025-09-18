# ReplicationSlotsComputeLogicalRestartLSN

## Location
src/backend/replication/slot.c: 1154 - 1211

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
  - LWLockAcquire (with LW_SHARED mode)
  - LWLockRelease
  - SpinLockAcquire
  - SpinLockRelease
  - SlotIsLogical
  - [ReplicationSlot](ReplicationSlot.md) (struct access)
  - RS_INVAL_NONE (invalidation state constant)

- Called from (representative examples):
  - CheckPointLogicalRewriteHeap
  - [CheckPointSnapBuild](../C/CheckPointSnapBuild.md)

## Notes and Other Information
- Specifically designed for logical decoding purposes, filtering out physical replication slots
- Results are computed on-demand rather than cached, unlike some other slot computation functions
- Early return with InvalidXLogRecPtr if max_replication_slots <= 0 (logical decoding disabled)
- Properly handles the case where restart_lsn might be InvalidXLogRecPtr for individual slots
- Uses the same locking strategy as ReplicationSlotsComputeRequiredLSN but with additional logical slot filtering