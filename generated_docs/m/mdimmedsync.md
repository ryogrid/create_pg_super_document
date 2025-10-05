# mdimmedsync

## Location
[src/backend/storage/smgr/md.c:1293-1354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1293-L1354)

## Overview
Immediately syncs a relation to stable storage, ensuring all segments (both active and inactive) are flushed to disk.

## Definition

```c
void
mdimmedsync(SMgrRelation reln, ForkNumber forknum)
```
## Detailed Description
The mdimmedsync function performs an immediate synchronization of a relation to stable storage. It operates by syncing all segments of a relation, including both active and inactive segments. This is crucial for maintaining data integrity, especially in scenarios where WAL is skipped or during recovery operations.

The function first ensures all active segments are opened by calling mdnblocks, then temporarily opens any inactive segments. It iterates through all segments from the highest numbered segment down to segment 0, performing an fsync on each. After syncing, inactive segments are immediately closed to free resources.

This function is particularly important for handling cases where a relation might skip WAL logging, and ensures that even segments that have been truncated or made inactive are properly synced to prevent data corruption during recovery.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer representing the storage manager relation to sync
- `forknum`: ForkNumber indicating which fork of the relation to sync (main, FSM, VM, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - [mdnblocks](mdnblocks.md)
  - [_mdfd_openseg](_mdfd_openseg.md)
  - [FileSync](../F/FileSync.md)
  - [data_sync_elevel](../d/data_sync_elevel.md)
  - [FilePathName](../F/FilePathName.md)
  - [FileClose](../F/FileClose.md)
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

## Simplified Source

```c
void mdimmedsync(SMgrRelation reln, ForkNumber forknum)
{
    int segno;
    int min_inactive_seg;

    // Ensure all active segments are opened
    mdnblocks(reln, forknum);

    min_inactive_seg = segno = reln->md_num_open_segs[forknum];

    // Temporarily open any inactive segments beyond active ones
    while (_mdfd_openseg(reln, forknum, segno, 0) != NULL)
        segno++;

    // Process all segments in reverse order, syncing each to disk
    while (segno > 0)
    {
        MdfdVec *v = &reln->md_seg_fds[forknum][segno - 1];

        // Perform immediate fsync on the segment
        if (FileSync(v->mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
            ereport(data_sync_elevel(ERROR),
                    (errcode_for_file_access(),
                     errmsg("could not fsync file \"%s\": %m",
                            FilePathName(v->mdfd_vfd))));

        // Close inactive segments immediately to free resources
        if (segno > min_inactive_seg)
        {
            FileClose(v->mdfd_vfd);
            _fdvec_resize(reln, forknum, segno - 1);
        }

        segno--;
    }
}
```