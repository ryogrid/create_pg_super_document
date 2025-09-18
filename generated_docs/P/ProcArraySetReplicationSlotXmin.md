# ProcArraySetReplicationSlotXmin

## Location
src/backend/storage/ipc/procarray.c: 3942 - 3966

## Overview
Sets the minimum transaction ID (xmin) limits for replication slots to prevent vacuum and HOT pruning from removing rows still needed by replication slot clients.

## Definition
```c
void ProcArraySetReplicationSlotXmin(TransactionId xmin, TransactionId catalog_xmin, bool already_locked)
```

## Detailed Description
This function installs transaction ID limits that influence future computations of the xmin horizon to protect rows required by replication slots. It updates the global process array with the minimum transaction IDs that replication slots still need, ensuring that vacuum operations and HOT pruning do not remove data that replication clients may still need to read.

The function operates by setting two key fields in the process array:
- replication_slot_xmin: protects regular table data
- replication_slot_catalog_xmin: protects system catalog data

These values act as a floor for vacuum operations, preventing removal of tuple versions that are newer than these transaction IDs.

## Parameters / Member Variables
- `xmin`: The minimum transaction ID for regular table data that must be preserved for replication slots
- `catalog_xmin`: The minimum transaction ID for catalog data that must be preserved for replication slots  
- `already_locked`: Boolean indicating whether the caller already holds ProcArrayLock to avoid double-locking

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMe (for lock state verification)
  - LWLockAcquire/LWLockRelease (for process array synchronization)
  - elog (for debug logging)
- Called from (representative examples):
  - ReplicationSlotsComputeRequiredXmin (in slot.c)

## Notes and Other Information
- Uses ProcArrayLock in exclusive mode to ensure atomic updates of xmin values
- The already_locked parameter allows for optimization when caller already holds the required lock
- Debug logging shows the xmin values being set for both data and catalog
- These xmin values are consulted by vacuum and HOT pruning operations to determine what can be safely removed
- Critical for logical replication where slots need to retain historical data for decoding