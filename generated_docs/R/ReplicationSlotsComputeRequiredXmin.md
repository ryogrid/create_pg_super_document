# ReplicationSlotsComputeRequiredXmin

## Location
[src/backend/replication/slot.c:1049-1104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1049-L1104)

## Overview
Computes the oldest transaction IDs (xmin) across all replication slots and updates the global ProcArray to control VACUUM behavior.

## Definition

```c
void
ReplicationSlotsComputeRequiredXmin(bool already_locked)
```
## Detailed Description
This function performs a critical role in PostgreSQL's MVCC (Multi-Version Concurrency Control) system by determining the oldest transaction ID that must be preserved across all active replication slots. It scans all replication slots to find the minimum effective_xmin and effective_catalog_xmin values, which represent the oldest transactions that logical or physical replication still needs to see.

The function operates in two phases: first, it iterates through all replication slots while holding the ReplicationSlotControlLock in shared mode to collect the minimum transaction IDs. Then it calls ProcArraySetReplicationSlotXmin to update the global transaction management system, which prevents VACUUM from removing tuple versions that replication slots still need.

This is essential for maintaining data consistency in replication scenarios - without this mechanism, VACUUM might remove old tuple versions before logical replication has had a chance to process them, leading to replication failures.

## Parameters / Member Variables
- : Boolean indicating whether ProcArrayLock has already been acquired exclusively by the caller

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (ReplicationSlotControlLock)
  - LWLockRelease
  - SpinLockAcquire/SpinLockRelease
  - TransactionIdIsValid
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [ProcArraySetReplicationSlotXmin](../P/ProcArraySetReplicationSlotXmin.md)
  - RS_INVAL_NONE (enum value)
  - [ReplicationSlot](ReplicationSlot.md) (struct type)
- Called from (representative examples):
  - CreateInitDecodingContext
  - LogicalConfirmReceivedLocation
  - [ReplicationSlotRelease](ReplicationSlotRelease.md)
  - [ReplicationSlotDropPtr](ReplicationSlotDropPtr.md)
  - [InvalidateObsoleteReplicationSlots](../I/InvalidateObsoleteReplicationSlots.md)
  - [PhysicalReplicationSlotNewXmin](../P/PhysicalReplicationSlotNewXmin.md)

## Notes and Other Information
- Skips invalidated slots (those marked with invalidation reasons other than RS_INVAL_NONE)
- Only considers slots that are currently in use
- Uses shared locking on ReplicationSlotControlLock to allow concurrent reads
- The already_locked parameter optimizes cases where ProcArrayLock is already held
- Critical for preventing premature VACUUM cleanup of data needed by replication
- Called whenever slot state changes that might affect the global xmin requirement
- Uses TransactionIdPrecedes to handle transaction ID wraparound correctly
- Updates both regular xmin and catalog_xmin for different types of replication needs