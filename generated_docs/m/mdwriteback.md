# mdwriteback

## Location
[src/backend/storage/smgr/md.c:1030-1088](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1030-L1088)

## Overview
mdwriteback tells the kernel to write pages back to storage, providing an efficient way to flush multiple consecutive blocks from buffer cache to disk.

## Definition

```c
void
mdwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks)
```
## Detailed Description
mdwriteback implements PostgreSQL's writeback mechanism for the magnetic disk (md) storage manager. It advises the operating system kernel to write dirty pages from the buffer cache back to persistent storage. The function is optimized to handle ranges of blocks efficiently, issuing as few flush requests as possible while respecting segment boundaries. It includes safety mechanisms to handle cases where relation files might have been removed, avoiding race conditions with concurrent operations like PROCSIGNAL_BARRIER_SMGRRELEASE.

The function works by splitting flush requests at segment boundaries since PostgreSQL relations are stored as separate files per segment. It uses the kernel's writeback facilities through FileWriteback to hint that specific ranges of data should be written to storage.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer representing the relation to flush
- `forknum`: ForkNumber identifying which fork of the relation to flush
- `blocknum`: BlockNumber specifying the starting block position for writeback
- `nblocks`: BlockNumber indicating the number of consecutive blocks to flush
## Dependencies
- Functions called/Symbols referenced:
  - [_mdfd_getseg](_mdfd_getseg.md)
  - [FileWriteback](../F/FileWriteback.md)
- Called from (representative examples):
  - Storage manager layer functions (via MD_H interface)

## Notes and Other Information
- Includes an assertion that direct I/O is not enabled (IO_DIRECT_DATA must be 0)
- Gracefully handles cases where relation segments might have been removed
- Avoids re-opening segment files that weren't already open to prevent race conditions
- Splits flush operations at segment boundaries for optimal performance
- Uses WAIT_EVENT_DATA_FILE_FLUSH for wait event reporting during flush operations
- The function is designed for advisory flushing - it hints to the kernel but doesn't guarantee immediate disk writes
- More efficient than individual block flushes when dealing with consecutive blocks

## Simplified Source

```c
void
mdwriteback(SMgrRelation reln, ForkNumber forknum,
            BlockNumber blocknum, BlockNumber nblocks)
{
    // Process flush requests in chunks, splitting at segment boundaries
    while (nblocks > 0) {
        BlockNumber nflush = nblocks;
        off_t seekpos;
        MdfdVec *v;
        int segnum_start, segnum_end;

        // Get segment - don't open if not already open
        v = _mdfd_getseg(reln, forknum, blocknum, true, EXTENSION_DONT_OPEN);

        // Handle case where segment file was removed
        if (!v)
            return;

        // Calculate segment boundaries
        segnum_start = blocknum / RELSEG_SIZE;
        segnum_end = (blocknum + nblocks - 1) / RELSEG_SIZE;

        // If spanning segments, flush only to end of current segment
        if (segnum_start != segnum_end)
            nflush = RELSEG_SIZE - (blocknum % ((BlockNumber) RELSEG_SIZE));

        // Calculate position within segment
        seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

        // Issue the flush request to the kernel
        FileWriteback(v->mdfd_vfd, seekpos, (off_t) BLCKSZ * nflush,
                     WAIT_EVENT_DATA_FILE_FLUSH);

        // Move to next chunk
        nblocks -= nflush;
        blocknum += nflush;
    }
}
```