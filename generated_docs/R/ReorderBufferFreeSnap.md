# ReorderBufferFreeSnap

## Location
[src/backend/replication/logical/reorderbuffer.c:1910-1924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L1910-L1924)

## Overview
ReorderBufferFreeSnap frees a previously copied snapshot used in logical replication, handling both copied snapshots and reference-counted snapshots appropriately.

## Definition

```c
static void
ReorderBufferFreeSnap(ReorderBuffer *rb, Snapshot snap)
```
## Detailed Description
This function is responsible for proper cleanup of snapshots used in PostgreSQL's logical replication system. It handles two different types of snapshots:
1. Copied snapshots (snap->copied is true) - These are freed directly using pfree()
2. Reference-counted snapshots (snap->copied is false) - These have their reference count decremented via SnapBuildSnapDecRefcount()

The function provides a unified interface for snapshot cleanup regardless of how the snapshot was originally obtained, ensuring proper memory management in the replication system.

## Parameters / Member Variables
- `*rb`: ReorderBuffer pointer - the reorder buffer context (currently unused in the implementation)
- `snap`: Snapshot pointer - the snapshot to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (for copied snapshots)
  - [SnapBuildSnapDecRefcount](../S/SnapBuildSnapDecRefcount.md) (for reference-counted snapshots)
- Called from (representative examples):
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md)
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md)
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md)
  - [ReorderBufferStreamTXN](ReorderBufferStreamTXN.md)

## Notes and Other Information
- This is a static function within reorderbuffer.c, indicating it's for internal use only
- The function complements ReorderBufferCopySnap which creates the snapshots
- Proper snapshot cleanup is critical for preventing memory leaks in long-running logical replication processes
- The rb parameter is currently unused but maintained for API consistency

## Simplified Source

```c
static void
ReorderBufferFreeSnap(ReorderBuffer *rb, Snapshot snap)
{
    // Handle different snapshot types appropriately
    if (snap->copied)
        pfree(snap);           // Directly free copied snapshots
    else
        SnapBuildSnapDecRefcount(snap);  // Decrement reference count for shared snapshots
}
```