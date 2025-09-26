# CopySnapshot

## Location
src/backend/utils/time/snapmgr.c: 574 - 629

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
  - SnapshotData
  - MemoryContextAlloc
- Called from (representative examples):
  - GetTransactionSnapshot
  - SetTransactionSnapshot
  - PushActiveSnapshotWithLevel
  - PushCopiedSnapshot
  - RegisterSnapshotOnOwner
  - ExportSnapshot

## Notes and Other Information
- This is a static function in snapmgr.c, not exposed as a public API
- The returned snapshot has regd_count and active_count initialized to 0
- The copied flag is set to true to distinguish from original snapshots  
- For overflowed subXID arrays, the subxip is only copied if the snapshot was taken during recovery (where all top-level XIDs are stored in subxip)
- Memory allocation uses a single block containing both the snapshot structure and XID arrays for efficiency
- The snapXactCompletionCount is reset to 0 in the copy