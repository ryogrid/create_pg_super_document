# mdwritev

## Location
[src/backend/storage/smgr/md.c:928-1029](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L928-L1029)

## Overview
mdwritev is a function that writes multiple database blocks at the appropriate locations within PostgreSQL relation files, specifically for updating already-existing blocks before the current EOF.

## Definition

```c
struct iovec iov[PG_IOV_MAX];
```
## Detailed Description
mdwritev performs vectorized block writing operations for PostgreSQL's storage manager (smgr) layer. It writes an array of buffers to consecutive block positions in a relation file, handling segment boundaries and ensuring proper error handling. The function is designed specifically for updating existing blocks and cannot extend relations beyond their current EOF. It uses efficient vectorized I/O operations through iovec structures and handles cases where writes might be incomplete due to system constraints.

The function processes blocks in segments, respecting PostgreSQL's file segmentation limits (RELSEG_SIZE), and uses the buffers_to_iovec utility to optimize I/O operations by merging contiguous buffers.

## Parameters / Member Variables
- : SMgrRelation pointer representing the relation to write to
- : ForkNumber identifying which fork of the relation (main, FSM, VM, etc.)
- : BlockNumber specifying the starting block position for writing
- : Array of void pointers pointing to the buffer data to be written
- : BlockNumber indicating the number of blocks to write
- : Boolean flag to skip fsync registration if true

## Dependencies
- Functions called/Symbols referenced:
  - [mdnblocks](mdnblocks.md)
  - [_mdfd_getseg](_mdfd_getseg.md)
  - [buffers_to_iovec](../b/buffers_to_iovec.md)
  - [FileWriteV](../F/FileWriteV.md)
  - [FilePathName](../F/FilePathName.md)
  - [compute_remaining_iovec](../c/compute_remaining_iovec.md)
  - SmgrIsTemp
  - [register_dirty_segment](../r/register_dirty_segment.md)
- Called from (representative examples):
  - Storage manager layer functions (via MD_H interface)

## Notes and Other Information
- This function includes extensive error handling for disk space issues (ENOSPC)
- Uses PostgreSQL's tracing system for performance monitoring (TRACE_POSTGRESQL_SMGR_MD_WRITE_START/DONE)
- Handles short writes by continuing in a loop until all data is transferred
- Respects segment boundaries and processes writes across multiple segments if necessary
- Only registers dirty segments for fsync if skipFsync is false and the relation is not temporary
- Contains debug assertions to verify that writes don't extend beyond the current EOF
- Uses vectorized I/O (iovec) for efficiency when writing multiple consecutive blocks

## Simplified Source

```c
void
mdwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
         const void **buffers, BlockNumber nblocks, bool skipFsync)
{
    while (nblocks > 0) {
        struct iovec iov[PG_IOV_MAX];
        int iovcnt;
        off_t seekpos;
        int nbytes;
        MdfdVec *v;
        BlockNumber nblocks_this_segment;
        size_t transferred_this_segment;
        size_t size_this_segment;

        // Get segment for this block
        v = _mdfd_getseg(reln, forknum, blocknum, skipFsync,
                        EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

        // Calculate position within segment
        seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

        // Calculate how many blocks to write in this segment
        nblocks_this_segment = Min(nblocks,
                                 RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE)));
        nblocks_this_segment = Min(nblocks_this_segment, lengthof(iov));

        // Set up iovec for vectored I/O
        iovcnt = buffers_to_iovec(iov, (void **) buffers, nblocks_this_segment);
        size_this_segment = nblocks_this_segment * BLCKSZ;
        transferred_this_segment = 0;

        // Write loop - handle short writes
        for (;;) {
            nbytes = FileWriteV(v->mdfd_vfd, iov, iovcnt, seekpos,
                              WAIT_EVENT_DATA_FILE_WRITE);

            // Handle write errors
            if (nbytes < 0) {
                bool enospc = errno == ENOSPC;
                ereport(ERROR, "could not write blocks %u..%u in file \"%s\": %m",
                       blocknum, blocknum + nblocks_this_segment - 1,
                       FilePathName(v->mdfd_vfd),
                       enospc ? "Check free disk space." : "");
            }

            // Check if we've written everything
            transferred_this_segment += nbytes;
            if (transferred_this_segment == size_this_segment)
                break;

            // Adjust for next iteration after short write
            seekpos += nbytes;
            iovcnt = compute_remaining_iovec(iov, iov, iovcnt, nbytes);
        }

        // Register for fsync if needed
        if (!skipFsync && !SmgrIsTemp(reln))
            register_dirty_segment(reln, forknum, v);

        // Move to next segment
        nblocks -= nblocks_this_segment;
        buffers += nblocks_this_segment;
        blocknum += nblocks_this_segment;
    }
}
```