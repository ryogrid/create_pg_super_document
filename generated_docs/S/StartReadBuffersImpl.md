# StartReadBuffersImpl

## Location
[src/backend/storage/buffer/bufmgr.c:1257-1351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1257-L1351)

## Overview
StartReadBuffersImpl is the core implementation function for PostgreSQL's asynchronous buffer reading system that prepares multiple buffers for I/O operations and optimizes read patterns by combining contiguous blocks.

## Definition

```c
static pg_attribute_always_inline bool
StartReadBuffersImpl(ReadBuffersOperation *operation,
					 Buffer *buffers,
					 BlockNumber blockNum,
					 int *nblocks,
					 int flags)
```
## Detailed Description
StartReadBuffersImpl is the heart of PostgreSQL's asynchronous buffer reading mechanism. It processes a range of block numbers, pinning buffers for each block and determining which ones need actual I/O operations. The function optimizes I/O by creating contiguous readable ranges - when it encounters a buffer hit (already in memory), it terminates the read operation to avoid creating multiple separate I/O operations. For blocks not in memory, it extends the readable range and prepares the operation for actual I/O. The function can optionally issue prefetch advice to the storage manager to optimize disk access patterns. It returns true if any I/O operations are needed (requiring a subsequent WaitReadBuffers call), or false if all requested blocks were found in memory.

## Parameters / Member Variables
- `*operation`: ReadBuffersOperation structure containing relation, storage manager, and strategy information
- `*buffers`: Output array to store the pinned Buffer handles
- `blockNum`: Starting block number for the read operation
- `*nblocks`: Input/output parameter specifying requested blocks on input, actual blocks processed on output
- `flags`: Control flags including READ_BUFFERS_ISSUE_ADVICE for prefetch optimization
## Dependencies
- Functions called/Symbols referenced:
  - [PinBufferForBlock](../P/PinBufferForBlock.md)
  - [smgrprefetch](../s/smgrprefetch.md)
  - likely (branch prediction hint)
  - MAX_IO_COMBINE_LIMIT (constant for I/O combining limit)
- Called from (representative examples):
  - [StartReadBuffers](StartReadBuffers.md)
  - [StartReadBuffer](StartReadBuffer.md)

## Notes and Other Information
- The function enforces MAX_IO_COMBINE_LIMIT to prevent excessive I/O combination that could hurt performance
- Read operations are terminated early when a buffer hit is encountered to maintain optimal I/O patterns - this prevents creating multiple disjointed readable ranges
- The READ_BUFFERS_ISSUE_ADVICE flag enables prefetch hints to the storage manager, though the current implementation may issue multiple advice calls across segment boundaries
- The function modifies the nblocks parameter to reflect the actual number of blocks processed, which may be less than requested due to early termination on buffer hits
- All buffers in the range are pinned regardless of whether they need I/O, ensuring consistent buffer management
- The function is marked always_inline for performance optimization in the critical I/O path
- The design simulates asynchronous I/O behavior while maintaining simplicity in the current implementation

## Simplified Source
```c
static bool
StartReadBuffersImpl(ReadBuffersOperation *operation,
                     Buffer *buffers,
                     BlockNumber blockNum,
                     int *nblocks,
                     int flags)
{
    int actual_nblocks = *nblocks;
    int io_buffers_len = 0;

    // Pin buffers for each requested block
    for (int i = 0; i < actual_nblocks; ++i) {
        bool found;

        // Pin buffer and check if already in memory
        buffers[i] = PinBufferForBlock(operation->rel,
                                       operation->smgr,
                                       operation->smgr_persistence,
                                       operation->forknum,
                                       blockNum + i,
                                       operation->strategy,
                                       &found);

        if (found) {
            // Buffer hit - terminate to avoid fragmented I/O
            actual_nblocks = i + 1;
            break;
        } else {
            // Buffer miss - extend readable range
            io_buffers_len++;
        }
    }

    *nblocks = actual_nblocks;

    // All blocks were in memory - no I/O needed
    if (io_buffers_len == 0)
        return false;

    // Prepare I/O operation
    operation->buffers = buffers;
    operation->blocknum = blockNum;
    operation->flags = flags;
    operation->nblocks = actual_nblocks;
    operation->io_buffers_len = io_buffers_len;

    // Issue prefetch advice if requested
    if (flags & READ_BUFFERS_ISSUE_ADVICE) {
        smgrprefetch(operation->smgr,
                     operation->forknum,
                     blockNum,
                     operation->io_buffers_len);
    }

    // I/O needed - caller should call WaitReadBuffers
    return true;
}
```