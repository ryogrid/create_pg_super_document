# CopySnapshot

## Location
[src/backend/utils/time/snapmgr.c:574-629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L574-L629)

## Overview
Creates a deep copy of an MVCC snapshot structure, allocating new memory and copying all transaction ID arrays.

## Definition
```c
static Snapshot CopySnapshot(Snapshot snapshot)
```

## Detailed Description
This function creates a complete copy of a snapshot structure, including all embedded transaction ID arrays. The copy is allocated in TopTransactionContext to ensure it persists for the duration of the transaction. The function carefully handles memory layout by allocating a single memory block that contains both the snapshot structure and any required XID arrays.

Key behaviors:
- Allocates memory in TopTransactionContext for transaction-lifetime persistence
- Copies both main XID array (xip) and sub-transaction XID array (subxip) 
- Resets reference counts and marks the snapshot as copied
- Optimizes memory layout by storing arrays immediately after the main structure
- Handles subXID array overflow conditions appropriately

## Parameters / Member Variables
- `snapshot`: The source snapshot to copy, must not be InvalidSnapshot

## Dependencies
- Functions called/Symbols referenced:
  - InvalidSnapshot
  - [SnapshotData](../S/SnapshotData.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
- Called from (representative examples):
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [SetTransactionSnapshot](../S/SetTransactionSnapshot.md)
  - [PushActiveSnapshotWithLevel](../P/PushActiveSnapshotWithLevel.md)
  - [PushCopiedSnapshot](../P/PushCopiedSnapshot.md)
  - [RegisterSnapshotOnOwner](../R/RegisterSnapshotOnOwner.md)
  - [ExportSnapshot](../E/ExportSnapshot.md)

## Notes and Other Information
- This is a static function in snapmgr.c, not exposed as a public API
- The returned snapshot has regd_count and active_count initialized to 0
- The copied flag is set to true to distinguish from original snapshots  
- For overflowed subXID arrays, the subxip is only copied if the snapshot was taken during recovery (where all top-level XIDs are stored in subxip)
- Memory allocation uses a single block containing both the snapshot structure and XID arrays for efficiency
- The snapXactCompletionCount is reset to 0 in the copy

## Simplified Source

```c
// Simplified version of CopySnapshot
static Snapshot CopySnapshot(Snapshot snapshot) {
    Snapshot newsnap;
    Size subxipoff;
    Size size;

    Assert(snapshot != InvalidSnapshot);

    // Calculate total size needed: snapshot + xip array + subxip array
    size = subxipoff = sizeof(SnapshotData) + snapshot->xcnt * sizeof(TransactionId);
    if (snapshot->subxcnt > 0) {
        size += snapshot->subxcnt * sizeof(TransactionId);
    }

    // Allocate memory for snapshot and arrays in one block
    newsnap = (Snapshot) MemoryContextAlloc(TopTransactionContext, size);
    memcpy(newsnap, snapshot, sizeof(SnapshotData));

    // Initialize copy-specific fields
    newsnap->regd_count = 0;
    newsnap->active_count = 0;
    newsnap->copied = true;
    newsnap->snapXactCompletionCount = 0;

    // Copy main XID array
    if (snapshot->xcnt > 0) {
        newsnap->xip = (TransactionId *) (newsnap + 1);
        memcpy(newsnap->xip, snapshot->xip, snapshot->xcnt * sizeof(TransactionId));
    } else {
        newsnap->xip = NULL;
    }

    // Copy sub-transaction XID array (skip if overflowed unless taken during recovery)
    if (snapshot->subxcnt > 0 &&
        (!snapshot->suboverflowed || snapshot->takenDuringRecovery)) {
        newsnap->subxip = (TransactionId *) ((char *) newsnap + subxipoff);
        memcpy(newsnap->subxip, snapshot->subxip, snapshot->subxcnt * sizeof(TransactionId));
    } else {
        newsnap->subxip = NULL;
    }

    return newsnap;
}
```

Key simplifications made:
- Consolidated memory allocation logic into clearer blocks
- Added comments explaining the memory layout strategy
- Simplified the subXID array handling conditions
- Preserved all essential functionality while improving readability
- Maintained the single-block allocation optimization