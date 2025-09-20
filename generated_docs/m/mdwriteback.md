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
- : SMgrRelation pointer representing the relation to flush
- : ForkNumber identifying which fork of the relation to flush
- : BlockNumber specifying the starting block position for writeback
- : BlockNumber indicating the number of consecutive blocks to flush

## Dependencies
- Functions called/Symbols referenced:
  - [_mdfd_getseg](_mdfd_getseg.md)
  - FileWriteback
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