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

## Simplified Source

```c
void WaitReadBuffers(ReadBuffersOperation *operation) {
    // Setup basic operation parameters
    int nblocks = operation->io_buffers_len;
    if (nblocks == 0) return;  // Nothing to read

    Buffer *buffers = &operation->buffers[0];
    BlockNumber blocknum = operation->blocknum;
    ForkNumber forknum = operation->forknum;

    // Determine I/O context based on relation persistence
    char persistence = operation->rel ? operation->rel->rd_rel->relpersistence : RELPERSISTENCE_PERMANENT;
    IOContext io_context = (persistence == RELPERSISTENCE_TEMP) ? IOCONTEXT_NORMAL : IOContextForStrategy(operation->strategy);
    IOObject io_object = (persistence == RELPERSISTENCE_TEMP) ? IOOBJECT_TEMP_RELATION : IOOBJECT_RELATION;

    // Update buffer usage statistics
    if (persistence == RELPERSISTENCE_TEMP)
        pgBufferUsage.local_blks_read += nblocks;
    else
        pgBufferUsage.shared_blks_read += nblocks;

    // Process each buffer that needs I/O
    for (int i = 0; i < nblocks; ++i) {
        Buffer io_buffers[MAX_IO_COMBINE_LIMIT];
        void *io_pages[MAX_IO_COMBINE_LIMIT];
        int io_buffers_len;
        BlockNumber io_first_block;

        // Skip if another backend already completed this I/O
        if (!WaitReadBuffersCanStartIO(buffers[i], false)) {
            TRACE_POSTGRESQL_BUFFER_READ_DONE(forknum, blocknum + i, ...);
            continue;
        }

        // Setup first buffer for I/O
        io_buffers[0] = buffers[i];
        io_pages[0] = BufferGetBlock(buffers[i]);
        io_first_block = blocknum + i;
        io_buffers_len = 1;

        // Combine consecutive blocks into single I/O operation
        while ((i + 1) < nblocks && WaitReadBuffersCanStartIO(buffers[i + 1], true)) {
            io_buffers[io_buffers_len] = buffers[++i];
            io_pages[io_buffers_len++] = BufferGetBlock(buffers[i]);
        }

        // Perform the actual I/O with timing
        instr_time io_start = pgstat_prepare_io_time(track_io_timing);
        smgrreadv(operation->smgr, forknum, io_first_block, io_pages, io_buffers_len);
        pgstat_count_io_op_time(io_object, io_context, IOOP_READ, io_start, io_buffers_len);

        // Validate and finalize each buffer
        for (int j = 0; j < io_buffers_len; ++j) {
            BufferDesc *bufHdr;
            Block bufBlock;

            // Get buffer descriptor and block data
            if (persistence == RELPERSISTENCE_TEMP) {
                bufHdr = GetLocalBufferDescriptor(-io_buffers[j] - 1);
                bufBlock = LocalBufHdrGetBlock(bufHdr);
            } else {
                bufHdr = GetBufferDescriptor(io_buffers[j] - 1);
                bufBlock = BufHdrGetBlock(bufHdr);
            }

            // Validate page and handle corruption
            if (!PageIsVerifiedExtended((Page) bufBlock, io_first_block + j, PIV_LOG_WARNING | PIV_REPORT_STAT)) {
                if ((operation->flags & READ_BUFFERS_ZERO_ON_ERROR) || zero_damaged_pages) {
                    ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                           errmsg("invalid page in block %u of relation %s; zeroing out page", ...)));
                    memset(bufBlock, 0, BLCKSZ);
                } else {
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                           errmsg("invalid page in block %u of relation %s", ...)));
                }
            }

            // Set buffer as valid and terminate I/O
            if (persistence == RELPERSISTENCE_TEMP) {
                uint32 buf_state = pg_atomic_read_u32(&bufHdr->state);
                buf_state |= BM_VALID;
                pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
            } else {
                TerminateBufferIO(bufHdr, false, BM_VALID, true);
            }

            TRACE_POSTGRESQL_BUFFER_READ_DONE(forknum, io_first_block + j, ...);
        }

        // Update vacuum cost accounting
        VacuumPageMiss += io_buffers_len;
        if (VacuumCostActive)
            VacuumCostBalance += VacuumCostPageMiss * io_buffers_len;
    }
}
```