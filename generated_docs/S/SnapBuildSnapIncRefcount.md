# SnapBuildSnapIncRefcount

## Location
src/backend/replication/logical/snapbuild.c: 455 - 466

## Overview
Increments the reference count of a snapshot to prevent premature deallocation when the snapshot is shared or handed out to external resources.

## Definition
```c
static void SnapBuildSnapIncRefcount(Snapshot snap)
```

## Detailed Description
This is a static utility function that implements reference counting for snapshots in the logical replication system. It increments the `active_count` field of the snapshot, which tracks how many references to the snapshot currently exist. This is essential for memory management in PostgreSQL's snapshot building system, where snapshots may be shared between multiple transactions or handed out to external resources. The reference counting mechanism ensures that snapshots are not freed while they are still being used, preventing use-after-free bugs and maintaining system stability.

## Parameters / Member Variables
- `snap`: Pointer to the Snapshot structure whose reference count should be incremented

## Dependencies
- Functions called/Symbols referenced:
  - (No external functions called - simple increment operation)
- Called from (representative examples):
  - [SnapBuild](SnapBuild.md) (various snapshot building functions)
  - [SnapBuildGetOrBuildSnapshot](SnapBuildGetOrBuildSnapshot.md)
  - [SnapBuildProcessChange](SnapBuildProcessChange.md)
  - [SnapBuildDistributeSnapshotAndInval](SnapBuildDistributeSnapshotAndInval.md)
  - SnapBuildCommitTxn
  - [SnapBuildRestore](SnapBuildRestore.md)

## Notes and Other Information
- Static function, only accessible within snapbuild.c
- Part of the reference counting memory management scheme for snapshots
- Must be paired with corresponding SnapBuildSnapDecRefcount calls
- Used when creating new snapshot references or passing snapshots to external code
- Critical for preventing memory leaks and use-after-free errors in logical replication
- Located in src/backend/replication/logical/snapbuild.c:455-466