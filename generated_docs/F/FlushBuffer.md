# FlushBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:3773-3911](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3773-L3911)

## Overview
FlushBuffer physically writes a shared buffer's contents to disk, implementing PostgreSQL's WAL-before-data rule and handling checksums, I/O tracking, and proper buffer state management.

## Definition
```c
static void FlushBuffer(BufferDesc *buf, SMgrRelation reln, IOObject io_object, IOContext io_context)
```

## Detailed Description
FlushBuffer is the core function responsible for writing dirty buffers to persistent storage. It implements several critical aspects of PostgreSQL's storage system: (1) WAL-before-data rule enforcement by flushing WAL up to the buffer's LSN before writing the page, (2) proper I/O state management using StartBufferIO/TerminateBufferIO to prevent concurrent flushes, (3) checksum calculation and handling of concurrent hint bit updates by copying the page when necessary, (4) special handling for unlogged relations that don't require WAL flushing, and (5) comprehensive I/O statistics tracking. The function assumes the caller holds a pin and share-lock on the buffer, allowing safe access to buffer contents while permitting hint bit updates during the write operation.

## Parameters / Member Variables
- `buf`: Buffer descriptor for the buffer to be flushed
- `reln`: SMgrRelation handle for the relation (can be NULL, will be opened if needed)
- `io_object`: I/O object type for statistics tracking
- `io_context`: I/O context for statistics tracking (normal, bulk read, bulk write, vacuum, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [StartBufferIO](../S/StartBufferIO.md)
  - [smgropen](../s/smgropen.md)
  - [BufTagGetRelFileLocator](../B/BufTagGetRelFileLocator.md)
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
  - [LockBufHdr](../L/LockBufHdr.md)/UnlockBufHdr
  - BufferGetLSN
  - [XLogFlush](../X/XLogFlush.md)
  - BufHdrGetBlock
  - [PageSetChecksumCopy](../P/PageSetChecksumCopy.md)
  - [smgrwrite](../s/smgrwrite.md)
  - [TerminateBufferIO](../T/TerminateBufferIO.md)
  - [pgstat_count_io_op_time](../p/pgstat_count_io_op_time.md)
- Called from (representative examples):
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - [SyncOneBuffer](../S/SyncOneBuffer.md)
  - [FlushRelationBuffers](FlushRelationBuffers.md)
  - [FlushDatabaseBuffers](FlushDatabaseBuffers.md)

## Notes and Other Information
- Implements WAL-before-data rule except for unlogged relations
- Handles page checksums and concurrent hint bit updates via page copying
- Uses I/O state management to prevent concurrent flushes of the same buffer
- Tracks comprehensive I/O statistics for different contexts (normal, strategy, etc.)
- Only passes data to kernel; actual disk write depends on kernel scheduling
- Critical for checkpoint operations and victim buffer replacement
- Includes error context callback for better error reporting

## Simplified Source

```c
// Simplified version of FlushBuffer
static void FlushBuffer(BufferDesc *buf, SMgrRelation reln, IOObject io_object, IOContext io_context) {
    XLogRecPtr recptr;
    uint32 buf_state;
    Block bufBlock;
    char *bufToWrite;

    // Try to start I/O operation - return if someone else flushed it first
    if (!StartBufferIO(buf, false, false)) {
        return;
    }

    // Setup error callback for better error reporting
    ErrorContextCallback errcallback;
    errcallback.callback = shared_buffer_write_error_callback;
    errcallback.arg = (void *) buf;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Open relation if not provided
    if (reln == NULL) {
        reln = smgropen(BufTagGetRelFileLocator(&buf->tag), INVALID_PROC_NUMBER);
    }

    // Get buffer LSN and clear just-dirtied flag
    buf_state = LockBufHdr(buf);
    recptr = BufferGetLSN(buf);
    buf_state &= ~BM_JUST_DIRTIED;
    UnlockBufHdr(buf, buf_state);

    // Enforce WAL-before-data rule for permanent relations
    if (buf_state & BM_PERMANENT) {
        XLogFlush(recptr);
    }

    // Get buffer data and compute checksum if needed
    bufBlock = BufHdrGetBlock(buf);
    bufToWrite = PageSetChecksumCopy((Page) bufBlock, buf->tag.blockNum);

    // Write buffer to disk
    instr_time io_start = pgstat_prepare_io_time(track_io_timing);
    smgrwrite(reln, BufTagGetForkNum(&buf->tag), buf->tag.blockNum, bufToWrite, false);

    // Update I/O statistics
    pgstat_count_io_op_time(IOOBJECT_RELATION, io_context, IOOP_WRITE, io_start, 1);
    pgBufferUsage.shared_blks_written++;

    // Mark buffer as clean and end I/O operation
    TerminateBufferIO(buf, true, 0, true);

    // Restore error context stack
    error_context_stack = errcallback.previous;
}
```

Key simplifications made:
- Removed detailed tracing and extensive comments
- Simplified error context setup
- Consolidated buffer state management
- Focused on core WAL-before-data logic
- Maintained essential I/O state management and statistics tracking
- Preserved checksum handling and buffer cleanup operations