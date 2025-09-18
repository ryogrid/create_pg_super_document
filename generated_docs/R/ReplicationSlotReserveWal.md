# ReplicationSlotReserveWal

## Location
src/backend/replication/slot.c: 1401 - 1476

## Overview
Reserves WAL (Write-Ahead Log) for the currently active replication slot by computing and setting an appropriate restart_lsn value.

## Definition
void ReplicationSlotReserveWal(void)

## Detailed Description
This function establishes WAL retention for a replication slot by setting its restart_lsn field. It handles both physical and logical slots differently:

For physical slots: Uses GetRedoRecPtr() to set restart_lsn to the last redo LSN, ensuring replay starts at the last logged checkpoint.

For logical slots: 
- On primary servers: Uses GetXLogInsertRecPtr() and logs a standby snapshot for faster startup
- On standby servers: Uses GetXLogReplayRecPtr() and waits for xl_running_xact records

The function implements a retry mechanism to handle race conditions where WAL segments might be removed concurrently during checkpoint operations. It continues looping until the required WAL is guaranteed to be available.

## Parameters / Member Variables
- None (operates on the global MyReplicationSlot)

## Dependencies
- Functions called/Symbols referenced:
  - SlotIsPhysical
  - GetRedoRecPtr
  - RecoveryInProgress
  - GetXLogReplayRecPtr
  - GetXLogInsertRecPtr
  - SpinLockAcquire/SpinLockRelease
  - ReplicationSlotsComputeRequiredLSN
  - XLByteToSeg
  - XLogGetLastRemovedSegno
  - SlotIsLogical
  - LogStandbySnapshot
  - XLogFlush
- Called from (representative examples):
  - CreateInitDecodingContext
  - create_physical_replication_slot
  - CreateReplicationSlot

## Notes and Other Information
Critical for preventing premature WAL removal that could break replication. The retry loop protects against race conditions with concurrent checkpoints. For logical slots on primary servers, it ensures a consistent snapshot is available for decoding startup by logging and flushing a standby snapshot.