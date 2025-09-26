# KnownAssignedXidsAdd

## Location
src/backend/storage/ipc/procarray.c: 4781 - 4884

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
  - TransactionIdPrecedesOrEquals (validates from_xid <= to_xid)
  - TransactionIdPrecedes (handles XID wraparound in range calculation)
  - TransactionIdAdvance (increments XIDs in the range)
  - TransactionIdFollowsOrEquals (verifies sequential insertion)
  - KnownAssignedXidsDisplay (for error logging)
  - KnownAssignedXidsCompress (compresses array when space is needed)
  - pg_write_barrier (ensures memory ordering without exclusive lock)
- Called from (representative examples):
  - ProcArrayApplyRecoveryInfo (during recovery info application)
  - RecordKnownAssignedTransactionIds (when recording new transactions)

## Notes and Other Information
- This is a static function only called by the startup process during recovery
- Handles XID wraparound by calculating nxids the "hard way" when to_xid < from_xid
- Enforces sequential insertion order to maintain array invariants
- Uses memory barriers to ensure visibility of array updates before head pointer changes
- May trigger array compression if insufficient space is available
- Throws ERROR if the array cannot accommodate the new XIDs even after compression
- Part of PostgreSQL's Hot Standby system for tracking active transactions during recovery