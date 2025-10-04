# XLogReadBufferForRedoExtended

## Location
[src/backend/access/transam/xlogutils.c:351-470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L351-L470)

## Overview
Extended version of XLogReadBufferForRedo that provides additional control over buffer reading modes and locking behavior during WAL replay, supporting page extension and specialized locking modes.

## Definition

```c
XLogRedoAction
XLogReadBufferForRedoExtended(XLogReaderState *record,
							  uint8 block_id,
							  ReadBufferMode mode, bool get_cleanup_lock,
							  Buffer *buf)
```
## Detailed Description
This is the comprehensive implementation underlying both XLogReadBufferForRedo and XLogInitBufferForRedo. It handles all aspects of reading buffers during WAL replay with fine-grained control over behavior.

Key functionality includes:
- **Full-page image restoration**: When a WAL record contains a full-page image with BKPIMAGE_APPLY flag, the page is restored completely
- **LSN-based replay logic**: Compares record EndRecPtr with page LSN to determine if replay is needed
- **Page extension**: In RBM_ZERO_* modes, extends relations with zero-filled pages if the target block doesn't exist
- **Validation**: Ensures WILL_INIT flag consistency between WAL record and buffer mode
- **Special handling**: INIT_FORKNUM buffers are force-flushed to maintain disk synchronization

The function returns different XLogRedoAction values indicating what action the caller should take based on the page state and WAL record content.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record being processed
- `block_id`: ID number identifying which block from the WAL record to read
- `mode`: ReadBufferMode specifying buffer access behavior (RBM_NORMAL, RBM_ZERO_AND_LOCK, etc.)
- `get_cleanup_lock`: If true, acquires cleanup lock instead of regular exclusive lock
- `*buf`: Output parameter receiving the buffer containing the requested page
## Dependencies
- Functions called/Symbols referenced:
  - [XLogRecGetBlockTagExtended](XLogRecGetBlockTagExtended.md)
  - XLogRecBlockImageApply
  - [XLogReadBufferExtended](XLogReadBufferExtended.md)
  - [RestoreBlockImage](../R/RestoreBlockImage.md)
  - [PageSetLSN](../P/PageSetLSN.md), PageGetLSN, PageIsNew
  - [LockBufferForCleanup](../L/LockBufferForCleanup.md)
  - [FlushOneBuffer](../F/FlushOneBuffer.md)
- Constants used:
  - BLK_NEEDS_REDO, BLK_DONE, BLK_RESTORED, BLK_NOTFOUND
  - BKPBLOCK_WILL_INIT
  - RBM_ZERO_AND_LOCK, RBM_ZERO_AND_CLEANUP_LOCK
- Called from:
  - [XLogReadBufferForRedo](XLogReadBufferForRedo.md) (simplified wrapper)
  - [XLogInitBufferForRedo](XLogInitBufferForRedo.md) (initialization wrapper)
  - Various specialized redo functions requiring extended control

## Notes and Other Information
- This is the core implementation for all WAL buffer reading operations
- Enforces strict consistency between WAL record flags and requested buffer modes
- Full-page images always take precedence over existing page content for data integrity
- INIT_FORKNUM special handling ensures unlogged relation consistency
- Cleanup locks provide stronger concurrency control for vacuum-related operations
- The function handles all error conditions with appropriate PANIC messages for data integrity violations

## Simplified Source

```c
XLogRedoAction
XLogReadBufferForRedoExtended(XLogReaderState *record, uint8 block_id,
                             ReadBufferMode mode, bool get_cleanup_lock,
                             Buffer *buf)
{
    XLogRecPtr lsn = record->EndRecPtr;
    RelFileLocator rlocator;
    ForkNumber forknum;
    BlockNumber blkno;
    Buffer prefetch_buffer;
    Page page;

    // Get block location info from WAL record
    if (!XLogRecGetBlockTagExtended(record, block_id, &rlocator, &forknum, &blkno, &prefetch_buffer)) {
        elog(PANIC, "failed to locate backup block with ID %d in WAL record", block_id);
    }

    // Validate WILL_INIT flag consistency with buffer mode
    bool zeromode = (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK);
    bool willinit = (XLogRecGetBlock(record, block_id)->flags & BKPBLOCK_WILL_INIT) != 0;
    if (willinit && !zeromode || !willinit && zeromode) {
        elog(PANIC, "WILL_INIT flag mismatch with buffer mode");
    }

    // If full-page image should be restored, do it
    if (XLogRecBlockImageApply(record, block_id)) {
        *buf = XLogReadBufferExtended(rlocator, forknum, blkno,
                                     get_cleanup_lock ? RBM_ZERO_AND_CLEANUP_LOCK : RBM_ZERO_AND_LOCK,
                                     prefetch_buffer);
        page = BufferGetPage(*buf);

        // Restore the full-page image
        if (!RestoreBlockImage(record, block_id, page)) {
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                          errmsg_internal("%s", record->errormsg_buf)));
        }

        // Set LSN if page is not uninitialized
        if (!PageIsNew(page)) {
            PageSetLSN(page, lsn);
        }

        MarkBufferDirty(*buf);

        // Force flush INIT_FORKNUM for unlogged relations
        if (forknum == INIT_FORKNUM) {
            FlushOneBuffer(*buf);
        }

        return BLK_RESTORED;
    } else {
        // Read buffer normally without full-page image
        *buf = XLogReadBufferExtended(rlocator, forknum, blkno, mode, prefetch_buffer);

        if (BufferIsValid(*buf)) {
            // Lock buffer if not already locked by zero modes
            if (mode != RBM_ZERO_AND_LOCK && mode != RBM_ZERO_AND_CLEANUP_LOCK) {
                if (get_cleanup_lock) {
                    LockBufferForCleanup(*buf);
                } else {
                    LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);
                }
            }

            // Check if replay is needed by comparing LSNs
            if (lsn <= PageGetLSN(BufferGetPage(*buf))) {
                return BLK_DONE;  // Already applied
            } else {
                return BLK_NEEDS_REDO;  // Needs replay
            }
        } else {
            return BLK_NOTFOUND;  // Block was truncated
        }
    }
}
```