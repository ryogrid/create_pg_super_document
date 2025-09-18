# XLogSaveBufferForHint

## Location
src/backend/access/transam/xloginsert.c: 1065 - 1142

## Overview
XLogSaveBufferForHint writes a backup block to WAL when setting hint bits on a page that requires protection for crash recovery.

## Definition


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