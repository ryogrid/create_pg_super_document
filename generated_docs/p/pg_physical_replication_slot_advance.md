# pg_physical_replication_slot_advance

## Location
[src/backend/replication/slotfuncs.c:463-498](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slotfuncs.c#L463-L498)

## Overview
A helper function that advances a physical replication slot's restart LSN forward to a specified WAL position.

## Definition

```c
static XLogRecPtr
pg_physical_replication_slot_advance(XLogRecPtr moveto)
```
## Detailed Description
This function advances a physical replication slot's restart_lsn to the specified target LSN position. It performs a simple comparison against the current restart_lsn, ensuring that only forward movement is allowed. The function is designed to be called within the context of an acquired replication slot (MyReplicationSlot must be valid).

When advancing the slot, the function:
1. Compares the target position with the current restart_lsn
2. Updates the restart_lsn atomically using spinlock protection
3. Marks the slot as dirty to ensure persistence at the next checkpoint
4. Wakes up logical WAL senders that may be waiting on logical failover slots

The function ensures data consistency by marking the slot dirty, though the advanced position may still be lost in case of a crash before the next checkpoint.

## Parameters / Member Variables
- `moveto`: The target WAL LSN position to advance the slot to. Must not be InvalidXLogRecPtr.
## Dependencies
- Functions called/Symbols referenced:
  -  - Marks the slot as needing to be written to disk
  -  - Wakes up logical WAL senders waiting on failover slots
- Called from:
  -  - Main SQL function for advancing replication slots

## Notes and Other Information
- This is a static helper function, not directly accessible from SQL
- Requires MyReplicationSlot to be properly acquired before calling
- The function only moves the slot forward; backward movement is not supported
- Uses spinlock protection for thread-safe updates to the slot data
- The advanced position persists only after the next checkpoint completes
- Physical slots use restart_lsn as their primary advancement point, unlike logical slots which track confirmed_flush

## Simplified Source

```c
static XLogRecPtr pg_physical_replication_slot_advance(XLogRecPtr moveto) {
    XLogRecPtr startlsn = MyReplicationSlot->data.restart_lsn;
    XLogRecPtr retlsn = startlsn;

    Assert(moveto != InvalidXLogRecPtr);

    // Only advance forward, never backward
    if (startlsn < moveto) {
        // Atomically update the restart LSN
        SpinLockAcquire(&MyReplicationSlot->mutex);
        MyReplicationSlot->data.restart_lsn = moveto;
        SpinLockRelease(&MyReplicationSlot->mutex);

        retlsn = moveto;

        // Mark slot dirty for persistence at next checkpoint
        ReplicationSlotMarkDirty();

        // Wake up logical WAL senders waiting on failover slots
        PhysicalWakeupLogicalWalSnd();
    }

    return retlsn;
}
```