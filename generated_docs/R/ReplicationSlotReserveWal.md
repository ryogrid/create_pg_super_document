# ReplicationSlotReserveWal

## Location
[src/backend/replication/slot.c:1401-1476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1401-L1476)

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


## Dependencies
- Functions called/Symbols referenced:
  - SlotIsPhysical
  - [GetRedoRecPtr](../G/GetRedoRecPtr.md)
  - [RecoveryInProgress](RecoveryInProgress.md)
  - [GetXLogReplayRecPtr](../G/GetXLogReplayRecPtr.md)
  - [GetXLogInsertRecPtr](../G/GetXLogInsertRecPtr.md)
  - SpinLockAcquire/SpinLockRelease
  - [ReplicationSlotsComputeRequiredLSN](ReplicationSlotsComputeRequiredLSN.md)
  - XLByteToSeg
  - [XLogGetLastRemovedSegno](../X/XLogGetLastRemovedSegno.md)
  - SlotIsLogical
  - [LogStandbySnapshot](../L/LogStandbySnapshot.md)
  - [XLogFlush](../X/XLogFlush.md)
- Called from (representative examples):
  - [CreateInitDecodingContext](../C/CreateInitDecodingContext.md)
  - [create_physical_replication_slot](../c/create_physical_replication_slot.md)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)

## Notes and Other Information
Critical for preventing premature WAL removal that could break replication. The retry loop protects against race conditions with concurrent checkpoints. For logical slots on primary servers, it ensures a consistent snapshot is available for decoding startup by logging and flushing a standby snapshot.

## Simplified Source

```c
// Simplified version of ReplicationSlotReserveWal
void ReplicationSlotReserveWal(void) {
    ReplicationSlot *slot = MyReplicationSlot;

    Assert(slot != NULL);
    Assert(slot->data.restart_lsn == InvalidXLogRecPtr);

    // Retry loop to handle concurrent WAL removal
    while (true) {
        XLogSegNo segno;
        XLogRecPtr restart_lsn;

        // Determine restart LSN based on slot type and recovery state
        if (SlotIsPhysical(slot)) {
            // Physical slots start at last redo point
            restart_lsn = GetRedoRecPtr();
        } else if (RecoveryInProgress()) {
            // Logical slots on standby use replay position
            restart_lsn = GetXLogReplayRecPtr(NULL);
        } else {
            // Logical slots on primary use insert position
            restart_lsn = GetXLogInsertRecPtr();
        }

        // Set restart_lsn atomically
        SpinLockAcquire(&slot->mutex);
        slot->data.restart_lsn = restart_lsn;
        SpinLockRelease(&slot->mutex);

        // Update global required LSN to prevent WAL removal
        ReplicationSlotsComputeRequiredLSN();

        // Check if required WAL is still available
        XLByteToSeg(slot->data.restart_lsn, segno, wal_segment_size);
        if (XLogGetLastRemovedSegno() < segno) {
            break; // Success - WAL is available
        }
        // Retry if WAL was removed concurrently
    }

    // For logical slots on primary, log standby snapshot for faster startup
    if (!RecoveryInProgress() && SlotIsLogical(slot)) {
        XLogRecPtr flushptr = LogStandbySnapshot();
        XLogFlush(flushptr);
    }
}
```

Key simplifications made:
- Added clear comments for each step
- Simplified the retry logic explanation
- Maintained essential race condition protection
- Preserved all slot type handling