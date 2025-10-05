# _mdfd_getseg

## Location
[src/backend/storage/smgr/md.c:1596-1726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1596-L1726)

## Overview
Finds and returns the segment file descriptor for a relation fork containing a specified block, with flexible behavior for handling missing segments.

## Definition
```c
static MdfdVec *_mdfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno, bool skipFsync, int behavior)
```

## Detailed Description
_mdfd_getseg is a critical function in PostgreSQL's segmented file storage system that locates the appropriate segment file containing a specific block number. It calculates the target segment based on the block number and RELSEG_SIZE, then handles various scenarios for missing segments based on the behavior parameter.

The function supports multiple behaviors: failing with an error (EXTENSION_FAIL), returning NULL (EXTENSION_RETURN_NULL), creating missing segments (EXTENSION_CREATE), or only returning already-opened segments (EXTENSION_DONT_OPEN). During WAL recovery, it can create segments even when not explicitly requested to support replay of operations on deleted relations.

When creating segments, the function maintains the invariant that all segments except the last must be exactly RELSEG_SIZE blocks by padding with zeros if necessary. This is crucial for hash indexes and recovery scenarios where discontiguous extension may occur.

## Parameters / Member Variables
- `reln`: The storage manager relation to find a segment for
- `forknum`: The fork number identifying which fork to search
- `blkno`: The block number that determines which segment is needed
- `skipFsync`: Whether to skip fsync when creating new segments
- `behavior`: Flags controlling how missing segments are handled (EXTENSION_*)

## Dependencies
- Functions called/Symbols referenced:
  - [mdopenfork](mdopenfork.md) (opens the initial fork if none are open)
  - [_mdnblocks](_mdnblocks.md) (gets the number of blocks in a segment)
  - [palloc_aligned](../p/palloc_aligned.md) (allocates aligned zero buffer)
  - [mdextend](mdextend.md) (extends segments with padding)
  - [pfree](../p/pfree.md) (frees allocated memory)
  - [_mdfd_segpath](_mdfd_segpath.md) (constructs segment file paths)
  - [_mdfd_openseg](_mdfd_openseg.md) (opens new segment files)
  - ereport, elog (error reporting)
- Called from (representative examples):
  - [mdextend](mdextend.md) (src/backend/storage/smgr/md.c:489)
  - [mdzeroextend](mdzeroextend.md) (src/backend/storage/smgr/md.c:562)
  - [mdprefetch](mdprefetch.md) (src/backend/storage/smgr/md.c:730)
  - [mdreadv](mdreadv.md) (src/backend/storage/smgr/md.c:824)
  - [mdwritev](mdwritev.md) (src/backend/storage/smgr/md.c:947)

## Notes and Other Information
- Returns NULL or raises errors based on behavior flags when segments don't exist
- Maintains segment size invariants by padding incomplete segments with zeros
- Handles WAL recovery scenarios by creating segments even when not explicitly requested
- Iterates through all segments between the last opened and target segment
- Sets errno to ENOENT when returning NULL to help callers distinguish failure reasons
- Critical for all I/O operations in the MD storage manager as it locates the correct segment
- The behavior parameter provides fine-grained control over error handling and segment creation

## Simplified Source

```c
static MdfdVec *_mdfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
                            bool skipFsync, int behavior)
{
    MdfdVec *v;
    BlockNumber targetseg;
    BlockNumber nextsegno;

    // Calculate which segment contains the target block
    targetseg = blkno / ((BlockNumber) RELSEG_SIZE);

    // Return already-opened segment if available
    if (targetseg < reln->md_num_open_segs[forknum])
    {
        v = &reln->md_seg_fds[forknum][targetseg];
        return v;
    }

    // Don't open new segments if caller doesn't want it
    if (behavior & EXTENSION_DONT_OPEN)
        return NULL;

    // Start from last opened segment, or open first segment
    if (reln->md_num_open_segs[forknum] > 0)
        v = &reln->md_seg_fds[forknum][reln->md_num_open_segs[forknum] - 1];
    else
    {
        v = mdopenfork(reln, forknum, behavior);
        if (!v)
            return NULL;
    }

    // Open all segments from current to target
    for (nextsegno = reln->md_num_open_segs[forknum];
         nextsegno <= targetseg; nextsegno++)
    {
        BlockNumber nblocks = _mdnblocks(reln, forknum, v);
        int flags = 0;

        if (nblocks > ((BlockNumber) RELSEG_SIZE))
            elog(FATAL, "segment too big");

        // Create segment if requested or during recovery
        if ((behavior & EXTENSION_CREATE) ||
            (InRecovery && (behavior & EXTENSION_CREATE_RECOVERY)))
        {
            // Pad incomplete segments to maintain size invariant
            if (nblocks < ((BlockNumber) RELSEG_SIZE))
            {
                char *zerobuf = palloc_aligned(BLCKSZ, PG_IO_ALIGN_SIZE, MCXT_ALLOC_ZERO);
                mdextend(reln, forknum,
                        nextsegno * ((BlockNumber) RELSEG_SIZE) - 1,
                        zerobuf, skipFsync);
                pfree(zerobuf);
            }
            flags = O_CREAT;
        }
        else if (!(behavior & EXTENSION_DONT_CHECK_SIZE) &&
                 nblocks < ((BlockNumber) RELSEG_SIZE))
        {
            // Handle incomplete segments based on behavior
            if (behavior & EXTENSION_RETURN_NULL)
            {
                errno = ENOENT;
                return NULL;
            }
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not open file \"%s\" (target block %u): previous segment is only %u blocks",
                                  _mdfd_segpath(reln, forknum, nextsegno), blkno, nblocks)));
        }

        // Open the segment
        v = _mdfd_openseg(reln, forknum, nextsegno, flags);

        if (v == NULL)
        {
            if ((behavior & EXTENSION_RETURN_NULL) && FILE_POSSIBLY_DELETED(errno))
                return NULL;
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not open file \"%s\" (target block %u): %m",
                                  _mdfd_segpath(reln, forknum, nextsegno), blkno)));
        }
    }

    return v;
}
```