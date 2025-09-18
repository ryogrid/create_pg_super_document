# mdimmedsync

## Location
src/backend/storage/smgr/md.c: 1293 - 1354

## Overview
Immediately syncs a relation to stable storage, ensuring all segments (both active and inactive) are flushed to disk.

## Definition


## Detailed Description
The mdimmedsync function performs an immediate synchronization of a relation to stable storage. It operates by syncing all segments of a relation, including both active and inactive segments. This is crucial for maintaining data integrity, especially in scenarios where WAL is skipped or during recovery operations.

The function first ensures all active segments are opened by calling mdnblocks, then temporarily opens any inactive segments. It iterates through all segments from the highest numbered segment down to segment 0, performing an fsync on each. After syncing, inactive segments are immediately closed to free resources.

This function is particularly important for handling cases where a relation might skip WAL logging, and ensures that even segments that have been truncated or made inactive are properly synced to prevent data corruption during recovery.

## Parameters / Member Variables
- : SMgrRelation pointer representing the storage manager relation to sync
- : ForkNumber indicating which fork of the relation to sync (main, FSM, VM, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [mdnblocks](mdnblocks.md)
  - [_mdfd_openseg](_mdfd_openseg.md)
  - FileSync
  - data_sync_elevel
  - [FilePathName](../F/FilePathName.md)
  - FileClose
  - [_fdvec_resize](../f/_fdvec_resize.md)
- Called from (representative examples):
  - Referenced in MD_H header file

## Notes and Other Information
- Only syncs writes that have already been issued; does not handle dirty buffers in the buffer manager
- Temporarily opens inactive segments for syncing, then closes them to prevent resource leaks
- Uses WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC event for tracking fsync operations
- Critical for data integrity in scenarios involving WAL-skipping relations
- Handles error reporting for failed fsync operations with appropriate error codes and messages
- Part of the magnetic disk (md) storage manager implementation