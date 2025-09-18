# PendingUnlinkEntry

## Location
src/backend/storage/sync/sync.c: 68 - 77

## Overview
PendingUnlinkEntry is a structure that tracks file deletion requests that need to be postponed until after the next checkpoint in PostgreSQL's synchronization system.

## Definition


## Detailed Description
PendingUnlinkEntry is part of PostgreSQL's checkpoint and synchronization mechanism, specifically designed to manage file deletion operations that must be deferred until after checkpoint completion. This structure is used to maintain a list of files that are marked for deletion but cannot be immediately removed from the filesystem because they may still be needed during crash recovery scenarios.

The structure works in conjunction with the checkpoint process to ensure data integrity. Files marked for deletion are kept in a pending state until a checkpoint cycle completes, at which point it's safe to physically remove them from disk. This mechanism prevents premature deletion of files that might be required for crash recovery or transaction rollback operations.

The structure is used within the sync.c module as part of the broader synchronization framework that manages both fsync operations (PendingFsyncEntry) and unlink operations (PendingUnlinkEntry) in a coordinated manner.

## Parameters / Member Variables
- : A FileTag structure that uniquely identifies the file to be deleted, including handler information, fork number, relation file locator, and segment number
- : A cycle counter (CycleCtr) that records the checkpoint cycle number when the deletion request was made, used to determine when it's safe to perform the actual deletion
- : A boolean flag indicating whether the deletion request has been canceled and should not be executed

## Dependencies
- Functions called/Symbols referenced:
  - FileTag (structure for file identification)
  - CycleCtr (typedef for cycle counter)
  - [HTAB](../H/HTAB.md) (hash table infrastructure)
- Called from (representative examples):
  - [SyncPostCheckpoint](../S/SyncPostCheckpoint.md) (processes pending unlinks after checkpoint)
  - [RememberSyncRequest](../R/RememberSyncRequest.md) (adds new unlink requests to the pending list)

## Notes and Other Information
- [PendingUnlinkEntry](PendingUnlinkEntry.md) is used specifically for non-temporary relations, as temporary files don't require the same careful deletion coordination
- The structure is managed through a linked list (pendingUnlinks) rather than a hash table, unlike fsync operations, because duplicate unlink requests are not expected
- The pending unlink mechanism ensures crash recovery safety by preventing premature file deletion
- This is part of the larger synchronization system that coordinates between regular backends and the checkpointer process
- The cycle counter mechanism allows the system to track which deletion requests are safe to execute based on checkpoint completion