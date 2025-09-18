# FlushBuffer

## Location
src/backend/storage/buffer/bufmgr.c: 3773 - 3911

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
  - StartBufferIO
  - [smgropen](../s/smgropen.md)
  - [BufTagGetRelFileLocator](../B/BufTagGetRelFileLocator.md)
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
  - LockBufHdr/UnlockBufHdr
  - BufferGetLSN
  - [XLogFlush](../X/XLogFlush.md)
  - BufHdrGetBlock
  - [PageSetChecksumCopy](../P/PageSetChecksumCopy.md)
  - smgrwrite
  - TerminateBufferIO
  - [pgstat_count_io_op_time](../p/pgstat_count_io_op_time.md)
- Called from (representative examples):
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - SyncOneBuffer
  - [FlushRelationBuffers](FlushRelationBuffers.md)
  - FlushDatabaseBuffers

## Notes and Other Information
- Implements WAL-before-data rule except for unlogged relations
- Handles page checksums and concurrent hint bit updates via page copying
- Uses I/O state management to prevent concurrent flushes of the same buffer
- Tracks comprehensive I/O statistics for different contexts (normal, strategy, etc.)
- Only passes data to kernel; actual disk write depends on kernel scheduling
- Critical for checkpoint operations and victim buffer replacement
- Includes error context callback for better error reporting