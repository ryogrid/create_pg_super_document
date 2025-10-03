# StartReadBuffers

## Location
[src/backend/storage/buffer/bufmgr.c:1352-1366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1352-L1366)

## Overview
StartReadBuffers is the public API function for initiating asynchronous read operations on multiple contiguous database blocks, serving as a wrapper around StartReadBuffersImpl with comprehensive documentation.

## Definition

```c
bool
StartReadBuffers(ReadBuffersOperation *operation,
				 Buffer *buffers,
				 BlockNumber blockNum,
				 int *nblocks,
				 int flags)
```
## Detailed Description
StartReadBuffers is the primary public interface for PostgreSQL's asynchronous buffer reading system. It initiates read operations for a range of blocks starting at blockNum and extending for *nblocks blocks. The function pins buffers for the requested blocks and determines whether actual I/O operations are needed. It serves as a thin wrapper around StartReadBuffersImpl, providing the same functionality with extensive API documentation. The function supports batched I/O operations and can issue prefetch advice when requested. Currently, the actual I/O is performed synchronously in the subsequent WaitReadBuffers() call, but the design allows for future enhancement to true asynchronous I/O initiation.

## Parameters / Member Variables
- `*operation`: ReadBuffersOperation structure that the caller partially initializes with relation and strategy information
- `*buffers`: Output array where pinned Buffer handles will be stored (must remain valid until WaitReadBuffers)
- `blockNum`: Starting block number for the read range
- `*nblocks`: Input/output parameter - requested blocks on input, actual blocks processed on output
- `flags`: Control flags, including READ_BUFFERS_ISSUE_ADVICE for prefetch optimization
## Dependencies
- Functions called/Symbols referenced:
  - [StartReadBuffersImpl](StartReadBuffersImpl.md)
  - [ReadBuffersOperation](../R/ReadBuffersOperation.md) (structure)
- Called from (representative examples):
  - [read_stream_start_pending_read](../r/read_stream_start_pending_read.md)
  - BUFFER_LOCK_EXCLUSIVE (from buffer management header)

## Notes and Other Information
- The function returns false if no I/O is necessary (all blocks found in buffer cache), true if I/O operations have been initiated
- When true is returned, WaitReadBuffers() must be called with the same operation object before accessing the buffers
- The caller-supplied buffers array must remain valid until WaitReadBuffers() is called
- The actual number of blocks processed may be fewer than requested due to buffer hits or other optimizations
- Currently implements synchronous I/O with optional prefetch advice, but the architecture supports future asynchronous I/O enhancements
- This is the recommended interface for bulk buffer reading operations as opposed to individual ReadBuffer calls
- The function is designed to be part of a two-phase operation: StartReadBuffers() to initiate and WaitReadBuffers() to complete

## Simplified Source

```c
bool StartReadBuffers(ReadBuffersOperation *operation,
                     Buffer *buffers,
                     BlockNumber blockNum,
                     int *nblocks,
                     int flags) {
    // Wrapper function - delegate to implementation
    return StartReadBuffersImpl(operation, buffers, blockNum, nblocks, flags);
}
```