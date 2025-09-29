# XLogReadBufferExtended

## Location
[src/backend/access/transam/xlogutils.c:471-562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L471-L562)

## Overview
Low-level function that reads pages during WAL replay with extended functionality, handling buffer caching, relation extension, and invalid page detection with various buffer reading modes.

## Definition

```c
Buffer
XLogReadBufferExtended(RelFileLocator rlocator, ForkNumber forknum,
					   BlockNumber blkno, ReadBufferMode mode,
					   Buffer recent_buffer)
```
## Detailed Description
This function provides the core buffer reading functionality during WAL replay, comparable to ReadBufferExtended but with specialized behavior for recovery scenarios. It handles multiple reading modes with different behaviors for missing or invalid pages.

Key behaviors by mode:
- **RBM_NORMAL**: Returns InvalidBuffer for non-existent or all-zero pages, with invalid page logging
- **RBM_ZERO_***: Extends relations with zero-filled pages when blocks don't exist
- **RBM_NORMAL_NO_LOG**: Returns InvalidBuffer for missing pages without logging

The function includes optimizations for buffer cache hits via recent_buffer hints and ensures proper relation creation during replay. It validates page initialization and logs invalid pages for consistency checking.

Important: Redo functions should typically use XLogReadBufferForRedoExtended instead of calling this directly, as it ensures proper WAL record registration for page modifications.

## Parameters / Member Variables
- `rlocator`: RelFileLocator identifying the target relation
- `forknum`: Fork number (main, FSM, VM, or init fork)
- `blkno`: Block number within the fork to read
- `mode`: ReadBufferMode specifying read behavior and extension policy
- `recent_buffer`: Hint buffer that might contain the target page for optimization

## Dependencies
- Functions called/Symbols referenced:
  - [ReadRecentBuffer](../R/ReadRecentBuffer.md)
  - [smgropen](../s/smgropen.md), smgrcreate, smgrnblocks
  - [ReadBufferWithoutRelcache](../R/ReadBufferWithoutRelcache.md)
  - [ExtendBufferedRelTo](../E/ExtendBufferedRelTo.md)
  - [log_invalid_page](../l/log_invalid_page.md)
  - [PageIsNew](../P/PageIsNew.md), ReleaseBuffer
- Constants used:
  - RBM_NORMAL, RBM_NORMAL_NO_LOG, RBM_ZERO_*
  - INVALID_PROC_NUMBER
  - EB_PERFORMING_RECOVERY, EB_SKIP_EXTENSION_LOCK
- Called from:
  - [XLogReadBufferForRedoExtended](XLogReadBufferForRedoExtended.md) (primary caller)
  - [verifyBackupPageConsistency](../v/verifyBackupPageConsistency.md)
  - [XLogRecordPageWithFreeSpace](XLogRecordPageWithFreeSpace.md)

## Notes and Other Information
- Creates target files automatically to handle replay sequences with later-deleted relations
- Uses recent_buffer hints to optimize buffer pool lookups
- Extends relations during recovery without requiring extension locks
- Validates page initialization in RBM_NORMAL mode to detect corruption
- Invalid page logging supports end-of-recovery consistency verification
- The function assumes single-process recovery environment for some optimizations
- Proper error handling ensures data integrity during replay operations

## Simplified Source
```c
Buffer XLogReadBufferExtended(RelFileLocator rlocator, ForkNumber forknum,
                             BlockNumber blkno, ReadBufferMode mode,
                             Buffer recent_buffer)
{
    BlockNumber lastblock;
    Buffer buffer;
    SMgrRelation smgr;

    Assert(blkno != P_NEW);

    // Fast path: check if recent_buffer hint is valid
    if (BufferIsValid(recent_buffer) && mode == RBM_NORMAL &&
        ReadRecentBuffer(rlocator, forknum, blkno, recent_buffer)) {
        buffer = recent_buffer;
        goto validate_page;
    }

    // Open relation and ensure it exists
    smgr = smgropen(rlocator, INVALID_PROC_NUMBER);
    smgrcreate(smgr, forknum, true);

    lastblock = smgrnblocks(smgr, forknum);

    if (blkno < lastblock) {
        // Page exists, read it
        buffer = ReadBufferWithoutRelcache(rlocator, forknum, blkno, mode, NULL, true);
    } else {
        // Page doesn't exist
        if (mode == RBM_NORMAL) {
            log_invalid_page(rlocator, forknum, blkno, false);
            return InvalidBuffer;
        }
        if (mode == RBM_NORMAL_NO_LOG) {
            return InvalidBuffer;
        }

        // Extend relation with new pages
        Assert(InRecovery);
        buffer = ExtendBufferedRelTo(BMR_SMGR(smgr, RELPERSISTENCE_PERMANENT),
                                    forknum, NULL,
                                    EB_PERFORMING_RECOVERY | EB_SKIP_EXTENSION_LOCK,
                                    blkno + 1, mode);
    }

validate_page:
    if (mode == RBM_NORMAL) {
        // Check if page is properly initialized
        Page page = (Page) BufferGetPage(buffer);
        if (PageIsNew(page)) {
            ReleaseBuffer(buffer);
            log_invalid_page(rlocator, forknum, blkno, true);
            return InvalidBuffer;
        }
    }

    return buffer;
}
```