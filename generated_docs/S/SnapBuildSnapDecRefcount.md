# SnapBuildSnapDecRefcount

## Location
src/backend/replication/logical/snapbuild.c: 467 - 498

## Overview
Decrements the reference count of a snapshot and automatically frees the snapshot when the reference count reaches zero, providing safe memory management for shared snapshots.

## Definition
```c
void SnapBuildSnapDecRefcount(Snapshot snap)
```

## Detailed Description
This function implements the decrement side of the reference counting mechanism for snapshots in PostgreSQL's logical replication system. It performs several important safety checks before decrementing the reference count, ensuring that only valid historic MVCC snapshots are being managed, that they haven't been improperly modified, and that they aren't copied snapshots (which have different cleanup requirements). When the active_count reaches zero after decrementing, the function automatically calls SnapBuildFreeSnapshot to deallocate the snapshot's memory. This automatic cleanup prevents memory leaks while ensuring snapshots remain valid as long as they have active references.

## Parameters / Member Variables
- `snap`: Pointer to the Snapshot structure whose reference count should be decremented

## Dependencies
- Functions called/Symbols referenced:
  - SNAPSHOT_HISTORIC_MVCC
  - FirstCommandId
  - [SnapBuildFreeSnapshot](SnapBuildFreeSnapshot.md)
- Called from (representative examples):
  - [ReorderBufferTransferSnapToParent](../R/ReorderBufferTransferSnapToParent.md)
  - [ReorderBufferCleanupTXN](../R/ReorderBufferCleanupTXN.md)
  - [ReorderBufferFreeSnap](../R/ReorderBufferFreeSnap.md)
  - [FreeSnapshotBuilder](../F/FreeSnapshotBuilder.md)
  - SnapBuildCommitTxn
  - [SnapBuildRestore](SnapBuildRestore.md)

## Notes and Other Information
- Externally visible function (not static) for use by other replication components
- Includes extensive assertion checks to ensure snapshot integrity
- Automatically frees snapshot memory when reference count reaches zero
- Only works with SNAPSHOT_HISTORIC_MVCC type snapshots used in logical replication
- Prevents freeing of copied snapshots (which require different cleanup)
- Critical component of the snapshot lifecycle management system
- Located in src/backend/replication/logical/snapbuild.c:467-498