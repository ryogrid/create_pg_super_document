# ResourceOwnerForgetSnapshot

## Location
[src/backend/utils/time/snapmgr.c:182-192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L182-L192)

## Overview
A convenience wrapper function that unregisters a snapshot from a ResourceOwner, removing it from automatic cleanup tracking.

## Definition
```c
static inline void ResourceOwnerForgetSnapshot(ResourceOwner owner, Snapshot snap)
```

## Detailed Description
This static inline function serves as a convenience wrapper around the general ResourceOwnerForget function, specifically designed for snapshot management. It unregisters a previously tracked snapshot from a ResourceOwner, indicating that the snapshot should no longer be automatically cleaned up when the ResourceOwner is released. This is typically used when a snapshot is being manually released or transferred to a different ResourceOwner before the original owner completes its cleanup phase.

## Parameters / Member Variables
- `owner`: The ResourceOwner that currently tracks this snapshot reference
- `snap`: The Snapshot to be unregistered from automatic cleanup

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerForget
  - PointerGetDatum
  - snapshot_resowner_desc (static descriptor)
- Called from (representative examples):
  - UnregisterSnapshotFromOwner

## Notes and Other Information
- This is a static inline function, meaning its only visible within snapmgr.c and gets inlined at compile time
- Counterpart to ResourceOwnerRememberSnapshot - they should be used in pairs
- Used when manual control over snapshot lifecycle is needed, bypassing automatic cleanup
- Failure to call this function before manually releasing a snapshot can lead to double-free errors during ResourceOwner cleanup
- The snapshot_resowner_desc descriptor is the same one used by ResourceOwnerRememberSnapshot, ensuring consistent resource management