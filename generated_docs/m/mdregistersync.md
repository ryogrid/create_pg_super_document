# mdregistersync

## Location
[src/backend/storage/smgr/md.c:1242-1292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1242-L1292)

## Overview
mdregistersync marks an entire PostgreSQL relation as needing fsync by registering all segments (both active and inactive) for synchronization.

## Definition

```c
void
mdregistersync(SMgrRelation reln, ForkNumber forknum)
```
## Detailed Description
mdregistersync ensures that all segments of a relation fork are marked as dirty and will be synchronized to disk during the next checkpoint or fsync operation. The function works by first ensuring all active segments are opened (via mdnblocks), then temporarily opening any inactive segments that exist beyond the active ones. It registers each segment as dirty and immediately closes the inactive segments to avoid keeping too many file descriptors open.

The function is typically used when a relation needs to be fully synchronized, such as during recovery operations or when ensuring data durability for critical operations. It handles both active segments (which remain open) and inactive segments (which are opened, marked, and immediately closed).

## Parameters / Member Variables
- `reln`: SMgrRelation pointer representing the relation to mark for sync
- `forknum`: ForkNumber identifying which fork of the relation to sync
## Dependencies
- Functions called/Symbols referenced:
  - [mdnblocks](mdnblocks.md)
  - [_mdfd_openseg](_mdfd_openseg.md)
  - [register_dirty_segment](../r/register_dirty_segment.md)
  - [FileClose](../F/FileClose.md)
  - [_fdvec_resize](../f/_fdvec_resize.md)
- Called from (representative examples):
  - Storage manager layer functions (via MD_H interface)

## Notes and Other Information
- Uses mdnblocks() first to ensure all active segments are opened
- Temporarily opens inactive segments but closes them immediately after marking
- Does not clean up inactive segments that might remain open after errors, leaving that to the next mdclose()
- Processes segments in reverse order (from highest to lowest segment number)
- Distinguishes between active segments (kept open) and inactive segments (closed after marking)
- The function is designed to be robust - if some inactive segments remain open due to errors, it's considered harmless
- Essential for ensuring full relation durability during checkpoints and recovery scenarios

## Simplified Source

```c
void mdregistersync(SMgrRelation reln, ForkNumber forknum)
{
    int segno;
    int min_inactive_seg;

    // Ensure all active segments are opened
    mdnblocks(reln, forknum);

    min_inactive_seg = segno = reln->md_num_open_segs[forknum];

    // Temporarily open any inactive segments beyond active ones
    while (_mdfd_openseg(reln, forknum, segno, 0) != NULL)
        segno++;

    // Process all segments in reverse order, marking each as dirty
    while (segno > 0)
    {
        MdfdVec *v = &reln->md_seg_fds[forknum][segno - 1];

        // Mark segment as needing fsync
        register_dirty_segment(reln, forknum, v);

        // Close inactive segments immediately to avoid keeping too many FDs open
        if (segno > min_inactive_seg)
        {
            FileClose(v->mdfd_vfd);
            _fdvec_resize(reln, forknum, segno - 1);
        }

        segno--;
    }
}
```