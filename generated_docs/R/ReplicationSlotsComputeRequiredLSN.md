# ReplicationSlotsComputeRequiredLSN

## Location
src/backend/replication/slot.c: 1105 - 1153

## Overview
Computes the oldest restart LSN across all active replication slots and informs the WAL module of the minimum LSN that must be retained for replication purposes.

## Definition


## Detailed Description
This function iterates through all replication slots to find the oldest (minimum) restart LSN among all active, non-invalidated slots. The computed minimum LSN is then passed to the WAL module via XLogSetReplicationSlotMinimumLSN to ensure that WAL segments at or after this position are retained and not deleted prematurely.

The function acquires a shared lock on ReplicationSlotControlLock to safely read slot data, then examines each slot's restart_lsn while holding the slot's mutex for thread safety. Invalidated slots are skipped as they don't require WAL retention.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (with LW_SHARED mode)
  - LWLockRelease
  - SpinLockAcquire
  - SpinLockRelease
  - [XLogSetReplicationSlotMinimumLSN](../X/XLogSetReplicationSlotMinimumLSN.md)
  - [ReplicationSlot](ReplicationSlot.md) (struct access)
  - RS_INVAL_NONE (invalidation state constant)

- Called from (representative examples):
  - LogicalConfirmReceivedLocation
  - [ReplicationSlotDropPtr](ReplicationSlotDropPtr.md)
  - [ReplicationSlotReserveWal](ReplicationSlotReserveWal.md)
  - [InvalidateObsoleteReplicationSlots](../I/InvalidateObsoleteReplicationSlots.md)
  - [StartupReplicationSlots](../S/StartupReplicationSlots.md)
  - [PhysicalConfirmReceivedLocation](../P/PhysicalConfirmReceivedLocation.md)

## Notes and Other Information
- The function specifically notes that max_slot_wal_keep_size is theoretically relevant but not accounted for because the module doesn't know what to compare against
- Invalidated slots are deliberately excluded from the computation as they no longer need WAL retention
- The function uses proper locking hierarchy: first acquiring the control lock, then individual slot mutexes
- This is a critical function for WAL retention management in PostgreSQL replication