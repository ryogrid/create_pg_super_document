# PrintPinnedBufs

## Location
src/backend/storage/buffer/bufmgr.c: 4438 - 4481

## Overview
A debugging function that prints detailed information about only the currently pinned buffers (buffers with positive private reference counts) to the server log.

## Definition


## Detailed Description
This function provides a focused diagnostic view of the shared buffer pool by examining only buffers that are currently pinned by the local process. It iterates through all buffer descriptors but only logs information for buffers that have a positive private reference count, indicating they are actively pinned by the current backend process. This selective approach makes it particularly useful for debugging buffer pin/unpin issues, memory leaks, or understanding which buffers a process is currently holding.

The function outputs detailed information for each pinned buffer including the buffer index, free list linkage, relation file path (using permanent path format), block number, flags, shared reference count, and private reference count. Like PrintBufferDescs, this function uses elog(LOG, ...) to write information to the PostgreSQL server log and omits buffer header locking for diagnostic convenience.

## Parameters / Member Variables
- None (void function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - GetBufferDescriptor
  - BufferDescriptorGetBuffer
  - GetPrivateRefCount (called twice - for checking and displaying)
  - relpathperm
  - BufTagGetRelFileLocator
  - BufTagGetForkNum
- Types used:
  - BufferDesc
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