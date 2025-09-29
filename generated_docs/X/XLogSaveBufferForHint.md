# XLogSaveBufferForHint

## Location
[src/backend/access/transam/xloginsert.c:1065-1142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xloginsert.c#L1065-L1142)

## Overview
XLogSaveBufferForHint writes a backup block to WAL when setting hint bits on a page that requires protection for crash recovery.

## Definition

```c
XLogRecPtr
XLogSaveBufferForHint(Buffer buffer, bool buffer_std)
```
## Detailed Description
XLogSaveBufferForHint handles WAL logging for hint bit modifications on pages that need crash recovery protection. Unlike normal WAL operations that require exclusive locks, this function works with only a shared lock on the buffer by copying the page data before logging. It only writes to WAL if the page's LSN is at or before the current Redo pointer, indicating the page hasn't been fully written in the current checkpoint cycle. For standard page layouts, it optimizes by copying only the data outside the pd_lower/pd_upper hole to reduce WAL volume. Multiple backends may concurrently write the same page, which is acceptable for correctness.

## Parameters / Member Variables
- : The buffer containing the page being modified with hint bits
- : Whether the page follows the standard PostgreSQL page layout (enables hole optimization)

## Dependencies
- Functions called/Symbols referenced:
  - [GetRedoRecPtr](../G/GetRedoRecPtr.md) (gets current recovery checkpoint pointer)
  - [BufferGetLSNAtomic](../B/BufferGetLSNAtomic.md) (atomically reads page LSN with buffer header lock)
  - [BufferGetBlock](../B/BufferGetBlock.md) (gets page data pointer)
  - [BufferGetTag](../B/BufferGetTag.md) (extracts buffer's relation/fork/block info)
  - [XLogBeginInsert](XLogBeginInsert.md) (starts WAL record construction)
  - [XLogRegisterBlock](XLogRegisterBlock.md) (registers page data with WAL record)
  - [XLogInsert](XLogInsert.md) (finalizes and writes WAL record)
  - DELAY_CHKPT_START (checkpoint delay flag)
- Called from:
  - [MarkBufferDirtyHint](../M/MarkBufferDirtyHint.md) (when marking buffer dirty due to hint changes)

## Notes and Other Information
- Works with shared buffer locks unlike normal backup block mechanism
- Only logs if page LSN <= Redo pointer (not yet checkpointed)
- Copies page data to avoid concurrent modification race conditions
- Optimizes standard pages by skipping the pd_lower/pd_upper hole
- Returns WAL record LSN if written, InvalidXLogRecPtr if no write needed
- Multiple concurrent backends may write same page (acceptable for correctness)
- Uses XLOG_FPI_FOR_HINT WAL record type for hint-related full page images

## Simplified Source

```c
XLogRecPtr XLogSaveBufferForHint(Buffer buffer, bool buffer_std) {
    XLogRecPtr recptr = InvalidXLogRecPtr;
    XLogRecPtr lsn;
    XLogRecPtr RedoRecPtr;

    // Ensure checkpoint cannot change our view
    Assert((MyProc->delayChkptFlags & DELAY_CHKPT_START) != 0);

    // Get current redo checkpoint pointer
    RedoRecPtr = GetRedoRecPtr();

    // Get page LSN atomically (with buffer header lock)
    lsn = BufferGetLSNAtomic(buffer);

    // Only write WAL if page hasn't been checkpointed yet
    if (lsn <= RedoRecPtr) {
        int flags = 0;
        PGAlignedBlock copied_buffer;
        char *origdata = (char *) BufferGetBlock(buffer);
        RelFileLocator rlocator;
        ForkNumber forkno;
        BlockNumber blkno;

        // Copy buffer to avoid concurrent modifications
        if (buffer_std) {
            // Optimize standard pages by skipping hole between pd_lower/pd_upper
            Page page = BufferGetPage(buffer);
            uint16 lower = ((PageHeader) page)->pd_lower;
            uint16 upper = ((PageHeader) page)->pd_upper;

            memcpy(copied_buffer.data, origdata, lower);
            memcpy(copied_buffer.data + upper, origdata + upper, BLCKSZ - upper);
        } else {
            // Copy entire page for non-standard layouts
            memcpy(copied_buffer.data, origdata, BLCKSZ);
        }

        // Build and insert WAL record
        XLogBeginInsert();

        if (buffer_std)
            flags |= REGBUF_STANDARD;

        BufferGetTag(buffer, &rlocator, &forkno, &blkno);
        XLogRegisterBlock(0, &rlocator, forkno, blkno, copied_buffer.data, flags);

        recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI_FOR_HINT);
    }

    return recptr;
}
```