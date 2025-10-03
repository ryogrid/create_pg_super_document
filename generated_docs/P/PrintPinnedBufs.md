# PrintPinnedBufs

## Location
[src/backend/storage/buffer/bufmgr.c:4438-4481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4438-L4481)

## Overview
A debugging function that prints detailed information about only the currently pinned buffers (buffers with positive private reference counts) to the server log.

## Definition

```c
void
PrintPinnedBufs(void)
```
## Detailed Description
This function provides a focused diagnostic view of the shared buffer pool by examining only buffers that are currently pinned by the local process. It iterates through all buffer descriptors but only logs information for buffers that have a positive private reference count, indicating they are actively pinned by the current backend process. This selective approach makes it particularly useful for debugging buffer pin/unpin issues, memory leaks, or understanding which buffers a process is currently holding.

The function outputs detailed information for each pinned buffer including the buffer index, free list linkage, relation file path (using permanent path format), block number, flags, shared reference count, and private reference count. Like PrintBufferDescs, this function uses elog(LOG, ...) to write information to the PostgreSQL server log and omits buffer header locking for diagnostic convenience.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md)
  - [GetPrivateRefCount](../G/GetPrivateRefCount.md) (called twice - for checking and displaying)
  - relpathperm
  - [BufTagGetRelFileLocator](../B/BufTagGetRelFileLocator.md)
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
- Types used:
  - [BufferDesc](../B/BufferDesc.md)
  - Buffer
- Called from (representative examples):
  - RelationGetNumberOfBlocks (via header reference)

## Notes and Other Information
- This function is primarily for debugging buffer pinning issues
- Only displays buffers with positive private reference counts (pinned by current process)
- Does not acquire buffer header locks during inspection (noted in source comment)
- Uses relpathperm() instead of relpathbackend() for permanent relation path format
- More focused than PrintBufferDescs - shows only actively pinned buffers rather than all buffers
- Particularly useful for detecting buffer pin leaks or understanding buffer usage patterns
- The selective nature makes output more manageable when debugging specific pinning issues
- Output format is identical to PrintBufferDescs but filtered to show only relevant buffers