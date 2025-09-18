# mdregistersync

## Location
[src/backend/storage/smgr/md.c:1242-1292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1242-L1292)

## Overview
mdregistersync marks an entire PostgreSQL relation as needing fsync by registering all segments (both active and inactive) for synchronization.

## Definition


## Detailed Description
mdregistersync ensures that all segments of a relation fork are marked as dirty and will be synchronized to disk during the next checkpoint or fsync operation. The function works by first ensuring all active segments are opened (via mdnblocks), then temporarily opening any inactive segments that exist beyond the active ones. It registers each segment as dirty and immediately closes the inactive segments to avoid keeping too many file descriptors open.

The function is typically used when a relation needs to be fully synchronized, such as during recovery operations or when ensuring data durability for critical operations. It handles both active segments (which remain open) and inactive segments (which are opened, marked, and immediately closed).

## Parameters / Member Variables
- : SMgrRelation pointer representing the relation to mark for sync
- : ForkNumber identifying which fork of the relation to sync

## Dependencies
- Functions called/Symbols referenced:
  - [mdnblocks](mdnblocks.md)
  - [_mdfd_openseg](_mdfd_openseg.md)
  - [register_dirty_segment](../r/register_dirty_segment.md)
  - FileClose
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