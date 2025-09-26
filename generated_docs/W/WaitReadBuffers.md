# WaitReadBuffers

## Location
[src/backend/storage/buffer/bufmgr.c:1395-1593](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1395-L1593)

## Overview
WaitReadBuffers performs asynchronous batch reading of multiple database pages from storage, implementing scatter-gather I/O optimization and handling both local and shared buffer validation.

## Definition
```c
void WaitReadBuffers(ReadBuffersOperation *operation)
```

## Detailed Description
This function orchestrates the complex process of reading multiple buffers from disk storage efficiently. Key features include:

1. **Batch Processing**: Processes multiple buffers in a single operation to reduce I/O overhead
2. **Scatter-Gather I/O**: Combines consecutive blocks into single read operations using smgrreadv
3. **Buffer State Management**: Handles both local and shared buffers with appropriate locking and state transitions
4. **Error Handling**: Validates page integrity and provides options for handling corrupted data
5. **Statistics Tracking**: Updates buffer usage statistics and I/O timing metrics
6. **Vacuum Cost Accounting**: Tracks vacuum-related page misses for cost-based vacuum delay

The function implements a sophisticated algorithm that:
- Skips buffers already read by other backends
- Groups consecutive blocks for efficient vectored reads
- Validates page headers after reading
- Properly terminates I/O operations and sets buffer valid flags
- Handles different persistence levels (permanent, temporary, unlogged)

## Parameters / Member Variables
- `operation`: A ReadBuffersOperation structure containing:
  - `buffers`: Array of buffer identifiers to read
  - `nblocks`: Total number of blocks in the operation
  - `io_buffers_len`: Number of buffers that need I/O
  - `blocknum`: Starting block number
  - `forknum`: Fork number (main, FSM, VM, etc.)
  - `rel`: Relation information
  - `smgr`: Storage manager for the relation
  - `strategy`: Buffer replacement strategy
  - `flags`: Operation flags (e.g., READ_BUFFERS_ZERO_ON_ERROR)

## Dependencies
- Functions called/Symbols referenced:
  - [WaitReadBuffersCanStartIO](WaitReadBuffersCanStartIO.md)
  - [IOContextForStrategy](../I/IOContextForStrategy.md)
  - [BufferGetBlock](../B/BufferGetBlock.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [smgrreadv](../s/smgrreadv.md)
  - [PageIsVerifiedExtended](../P/PageIsVerifiedExtended.md)
  - [TerminateBufferIO](../T/TerminateBufferIO.md)
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [pgstat_prepare_io_time](../p/pgstat_prepare_io_time.md)
  - [pgstat_count_io_op_time](../p/pgstat_count_io_op_time.md)
- Constants used:
  - MAX_IO_COMBINE_LIMIT
  - RELPERSISTENCE_TEMP
  - BM_VALID
  - READ_BUFFERS_ZERO_ON_ERROR
- Called from (representative examples):
  - [read_stream_next_buffer](../r/read_stream_next_buffer.md)
  - [ReadBuffer_common](../R/ReadBuffer_common.md)

## Notes and Other Information
- Implements PostgreSQL's advanced I/O combining strategy to reduce system call overhead
- The function can handle up to MAX_IO_COMBINE_LIMIT consecutive blocks in a single I/O operation
- Supports zero_damaged_pages configuration for handling corrupted pages
- Uses atomic operations for local buffer state management and proper locking for shared buffers
- Includes comprehensive tracing support via TRACE_POSTGRESQL_BUFFER_READ_DONE
- The function is central to PostgreSQL's buffer management performance, especially for sequential scans
- Handles the complex interaction between buffer management, storage management, and statistics collection
- Critical for maintaining data integrity through page validation while optimizing I/O performance