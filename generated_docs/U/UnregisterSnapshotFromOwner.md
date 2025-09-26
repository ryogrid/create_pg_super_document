# UnregisterSnapshotFromOwner

## Location
[src/backend/utils/time/snapmgr.c:849-858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L849-L858)

## Overview
Decrements the reference count of a snapshot and removes the corresponding reference from a specified resource owner, handling the resource owner cleanup before delegating to the core unregistration logic.

## Definition
```c
void UnregisterSnapshotFromOwner(Snapshot snapshot, ResourceOwner owner)
```

## Detailed Description
UnregisterSnapshotFromOwner handles snapshot unregistration from a specific resource owner. It performs a two-step process: first, it removes the snapshot reference from the specified resource owner using ResourceOwnerForgetSnapshot, then it delegates to UnregisterSnapshotNoOwner for the actual reference count management and potential snapshot cleanup. This function provides the middle layer in the snapshot unregistration hierarchy, handling resource owner cleanup while delegating core snapshot management.

## Parameters / Member Variables
- `snapshot`: The snapshot to unregister. Can be NULL, which will be handled gracefully by returning without action.
- `owner`: The ResourceOwner from which to remove the snapshot reference.

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerForgetSnapshot](../R/ResourceOwnerForgetSnapshot.md)
  - [UnregisterSnapshotNoOwner](UnregisterSnapshotNoOwner.md)
- Called from (representative examples):
  - [UnregisterSnapshot](UnregisterSnapshot.md)
  - [closeLOfd](../c/closeLOfd.md)
  - [PortalDrop](../P/PortalDrop.md)
  - [PreCommit_Portals](../P/PreCommit_Portals.md)

## Notes and Other Information
- Returns immediately without action if passed NULL snapshot
- Handles resource owner cleanup before core snapshot management
- Part of the layered snapshot unregistration system
- Ensures proper cleanup of resource owner tracking
- Counterpart to RegisterSnapshotOnOwner
- Located in src/backend/utils/time/snapmgr.c:849-858