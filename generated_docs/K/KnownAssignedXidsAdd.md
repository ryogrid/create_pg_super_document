# KnownAssignedXidsAdd

## Location
[src/backend/storage/ipc/procarray.c:4781-4884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4781-L4884)

## Overview
Adds a range of transaction IDs to the KnownAssignedXids array at the head position, maintaining proper sequencing and handling memory constraints during recovery processing.

## Definition
```c
static void KnownAssignedXidsAdd(TransactionId from_xid, TransactionId to_xid,
                                bool exclusive_lock)
```

## Detailed Description
This static function adds a range of transaction IDs from from_xid to to_xid (inclusive) to the KnownAssignedXids array. It calculates the required number of slots, handles XID wraparound cases, verifies sequential insertion order, and performs array compression if needed to make space. The function ensures thread-safe insertion using memory barriers when not holding exclusive locks, and maintains the array's head/tail pointers appropriately. All insertions must occur in TransactionId sequence to maintain the array's invariants.

## Parameters / Member Variables
- `from_xid`: The starting transaction ID of the range to add (inclusive)
- `to_xid`: The ending transaction ID of the range to add (inclusive)  
- `exclusive_lock`: True if caller already holds ProcArrayLock exclusively, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md) (validates from_xid <= to_xid)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (handles XID wraparound in range calculation)
  - TransactionIdAdvance (increments XIDs in the range)
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md) (verifies sequential insertion)
  - [KnownAssignedXidsDisplay](KnownAssignedXidsDisplay.md) (for error logging)
  - [KnownAssignedXidsCompress](KnownAssignedXidsCompress.md) (compresses array when space is needed)
  - pg_write_barrier (ensures memory ordering without exclusive lock)
- Called from (representative examples):
  - [ProcArrayApplyRecoveryInfo](../P/ProcArrayApplyRecoveryInfo.md) (during recovery info application)
  - [RecordKnownAssignedTransactionIds](../R/RecordKnownAssignedTransactionIds.md) (when recording new transactions)

## Notes and Other Information
- This is a static function only called by the startup process during recovery
- Handles XID wraparound by calculating nxids the "hard way" when to_xid < from_xid
- Enforces sequential insertion order to maintain array invariants
- Uses memory barriers to ensure visibility of array updates before head pointer changes
- May trigger array compression if insufficient space is available
- Throws ERROR if the array cannot accommodate the new XIDs even after compression
- Part of PostgreSQL's Hot Standby system for tracking active transactions during recovery

## Simplified Source

```c
// Simplified version of KnownAssignedXidsAdd
static void KnownAssignedXidsAdd(TransactionId from_xid, TransactionId to_xid,
                                bool exclusive_lock)
{
    ProcArrayStruct *pArray = procArray;
    TransactionId next_xid;
    int head, nxids, i;

    // Calculate number of XIDs to add (handle wraparound case)
    if (to_xid >= from_xid) {
        nxids = to_xid - from_xid + 1;
    } else {
        // Handle XID wraparound by counting manually
        nxids = 1;
        next_xid = from_xid;
        while (TransactionIdPrecedes(next_xid, to_xid)) {
            nxids++;
            TransactionIdAdvance(next_xid);
        }
    }

    // Get current array positions
    head = pArray->headKnownAssignedXids;

    // Verify sequential insertion order
    if (head > pArray->tailKnownAssignedXids &&
        TransactionIdFollowsOrEquals(KnownAssignedXids[head - 1], from_xid)) {
        elog(ERROR, "out-of-order XID insertion in KnownAssignedXids");
    }

    // Compress array if insufficient space
    if (head + nxids > pArray->maxKnownAssignedXids) {
        KnownAssignedXidsCompress(KAX_NO_SPACE, exclusive_lock);
        head = pArray->headKnownAssignedXids;

        if (head + nxids > pArray->maxKnownAssignedXids) {
            elog(ERROR, "too many KnownAssignedXids");
        }
    }

    // Insert XIDs into the array
    next_xid = from_xid;
    for (i = 0; i < nxids; i++) {
        KnownAssignedXids[head] = next_xid;
        KnownAssignedXidsValid[head] = true;
        TransactionIdAdvance(next_xid);
        head++;
    }

    // Update counts and head pointer with memory barrier
    pArray->numKnownAssignedXids += nxids;

    if (!exclusive_lock) {
        pg_write_barrier();  // Ensure array updates are visible
    }

    pArray->headKnownAssignedXids = head;
}
```

Key simplifications made:
- Removed detailed comments and assertions for clarity
- Consolidated variable declarations
- Simplified error handling flow
- Removed detailed boundary checks and validations
- Focused on core algorithm: calculate range, check space, compress if needed, insert XIDs
- Maintained essential logic for XID wraparound handling and memory barriers