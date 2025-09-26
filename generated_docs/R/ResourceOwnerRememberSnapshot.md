# ResourceOwnerRememberSnapshot

## Location
src/backend/utils/time/snapmgr.c: 177 - 181

## Overview
A convenience wrapper function that registers a snapshot with a ResourceOwner to ensure proper cleanup when the resource owner is released.

## Definition
```c
static inline void ResourceOwnerRememberSnapshot(ResourceOwner owner, Snapshot snap)
```

## Detailed Description
This static inline function serves as a convenience wrapper around the general ResourceOwnerRemember function, specifically designed for snapshot management. It registers a snapshot with a ResourceOwner so that the snapshot will be automatically cleaned up when the ResourceOwner is released during transaction cleanup or error handling. The function uses the predefined snapshot_resowner_desc descriptor which configures snapshots to be released in the RESOURCE_RELEASE_AFTER_LOCKS phase with RELEASE_PRIO_SNAPSHOT_REFS priority.

## Parameters / Member Variables
- `owner`: The ResourceOwner that should track this snapshot reference
- `snap`: The Snapshot to be tracked and automatically released

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerRemember
  - PointerGetDatum
  - snapshot_resowner_desc (static descriptor)
- Called from (representative examples):
  - RegisterSnapshotOnOwner

## Notes and Other Information
- This is a static inline function, meaning its only visible within snapmgr.c and gets inlined at compile time
- Part of PostgreSQLs resource management system that ensures proper cleanup of snapshots
- The snapshot_resowner_desc descriptor ensures snapshots are released after locks but before other cleanup phases
- Snapshots tracked this way will be automatically released via ResOwnerReleaseSnapshot callback
- Must be paired with ResourceOwnerForgetSnapshot when the snapshot is no longer needed before the ResourceOwner is released