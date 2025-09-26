# StartReadBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:1367-1381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1367-L1381)

## Overview
StartReadBuffer is a specialized single-block version of the asynchronous buffer reading API, optimized for reading exactly one database block with simplified parameter handling.

## Definition

```c
bool
StartReadBuffer(ReadBuffersOperation *operation,
				Buffer *buffer,
				BlockNumber blocknum,
				int flags)
```
## Detailed Description
StartReadBuffer is a convenience function that provides a streamlined interface for reading a single database block asynchronously. It's essentially a specialized wrapper around StartReadBuffersImpl that is optimized for the common case of reading exactly one block. The function eliminates the need for callers to manage the nblocks parameter when only one block is needed, making the API simpler and potentially saving a few CPU instructions when called from other translation units due to specialization. Like its multi-block counterpart, it pins the buffer and determines whether actual I/O is needed, returning true if WaitReadBuffers() must be called to complete the operation.

## Parameters / Member Variables
- : ReadBuffersOperation structure containing relation, storage manager, and strategy information
- : Output pointer to store the single pinned Buffer handle
- : Block number to read
- : Control flags including READ_BUFFERS_ISSUE_ADVICE for prefetch optimization

## Dependencies
- Functions called/Symbols referenced:
  - [StartReadBuffersImpl](StartReadBuffersImpl.md)
  - [ReadBuffersOperation](../R/ReadBuffersOperation.md) (structure)
- Called from (representative examples):
  - [read_stream_next_buffer](../r/read_stream_next_buffer.md)
  - [ReadBuffer_common](../R/ReadBuffer_common.md)
  - BUFFER_LOCK_EXCLUSIVE (from buffer management header)

## Notes and Other Information
- The function is specialized for nblocks == 1, which may provide minor performance benefits over the general StartReadBuffers function
- Contains an assertion that verifies exactly one block was processed, since single block operations cannot be 'short'
- Returns false if the block is already in the buffer cache (no I/O needed), true if I/O has been initiated
- When true is returned, WaitReadBuffers() must be called before accessing the buffer contents
- This function provides a cleaner API for single-block reads compared to setting up the nblocks parameter for StartReadBuffers
- The optimization comes from compile-time specialization rather than runtime parameter checking
- Used extensively in the read stream infrastructure and general buffer management code paths