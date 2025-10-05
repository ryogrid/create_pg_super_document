# mdtruncate

## Location
[src/backend/storage/smgr/md.c:1153-1241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1153-L1241)

## Overview
mdtruncate truncates a PostgreSQL relation to a specified number of blocks, handling multi-segment files and ensuring proper resource management without memory allocation.

## Definition

```c
void
mdtruncate(SMgrRelation reln, ForkNumber forknum,
		   BlockNumber curnblk, BlockNumber nblocks)
```
## Detailed Description
mdtruncate safely reduces the size of a PostgreSQL relation by truncating it to the specified number of blocks. The function is designed to be memory-allocation-free, making it safe for use in critical sections. It operates by truncating segments starting from the last one, which simplifies memory management for the file descriptor array in case of errors.

The function handles three scenarios for each segment: complete removal (truncate to 0 but keep the file), partial truncation (truncate to exact size needed), or preservation (no action needed). It maintains PostgreSQL's invariant of keeping at least the first segment and handles the special case where nblocks is exactly a multiple of RELSEG_SIZE by keeping a zero-length final segment.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer representing the relation to truncate
- `forknum`: ForkNumber identifying which fork of the relation to truncate
- `curnblk`: BlockNumber indicating the current number of blocks (must be obtained while holding appropriate locks)
- `nblocks`: BlockNumber specifying the target number of blocks after truncation
## Dependencies
- Functions called/Symbols referenced:
  - relpath
  - [FileTruncate](../F/FileTruncate.md)
  - [FilePathName](../F/FilePathName.md)
  - SmgrIsTemp
  - [register_dirty_segment](../r/register_dirty_segment.md)
  - [FileClose](../F/FileClose.md)
  - [_fdvec_resize](../f/_fdvec_resize.md)
- Called from (representative examples):
  - Storage manager layer functions (via MD_H interface)

## Notes and Other Information
- Guaranteed not to allocate memory, making it safe for critical sections
- Requires that smgrnblocks() was called beforehand to ensure all segments are opened
- Caller must hold sufficient locks to prevent concurrent relation size changes
- Processes segments from last to first to simplify error handling
- Truncates rather than deletes inactive segment files for safety reasons
- Maintains the invariant that the first segment is never dropped
- Handles the special case where nblocks is a multiple of RELSEG_SIZE by keeping a zero-length segment
- Registers dirty segments for fsync unless the relation is temporary
- Includes validation to prevent truncation beyond current size (except during recovery)
- Uses proper error reporting with file paths and block counts for debugging

## Simplified Source

```c
void
mdtruncate(SMgrRelation reln, ForkNumber forknum,
           BlockNumber curnblk, BlockNumber nblocks)
{
    BlockNumber priorblocks;
    int curopensegs;

    // Validate truncation request
    if (nblocks > curnblk) {
        if (InRecovery)
            return;
        ereport(ERROR, "could not truncate file to %u blocks: it's only %u blocks now",
               nblocks, curnblk);
    }

    // No work needed if already at target size
    if (nblocks == curnblk)
        return;

    // Process segments from last to first
    curopensegs = reln->md_num_open_segs[forknum];
    while (curopensegs > 0) {
        MdfdVec *v;

        priorblocks = (curopensegs - 1) * RELSEG_SIZE;
        v = &reln->md_seg_fds[forknum][curopensegs - 1];

        if (priorblocks > nblocks) {
            // Segment is beyond target - truncate to 0 but keep file
            if (FileTruncate(v->mdfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
                ereport(ERROR, "could not truncate file \"%s\": %m",
                       FilePathName(v->mdfd_vfd));

            // Register for fsync if not temporary
            if (!SmgrIsTemp(reln))
                register_dirty_segment(reln, forknum, v);

            // Close and remove from fd array (except first segment)
            FileClose(v->mdfd_vfd);
            _fdvec_resize(reln, forknum, curopensegs - 1);
        }
        else if (priorblocks + ((BlockNumber) RELSEG_SIZE) > nblocks) {
            // This is the last segment to keep - truncate to exact size
            BlockNumber lastsegblocks = nblocks - priorblocks;

            if (FileTruncate(v->mdfd_vfd, (off_t) lastsegblocks * BLCKSZ,
                           WAIT_EVENT_DATA_FILE_TRUNCATE) < 0)
                ereport(ERROR, "could not truncate file \"%s\" to %u blocks: %m",
                       FilePathName(v->mdfd_vfd), nblocks);

            if (!SmgrIsTemp(reln))
                register_dirty_segment(reln, forknum, v);
        }
        else {
            // This segment and all earlier ones are still needed
            break;
        }

        curopensegs--;
    }
}
```