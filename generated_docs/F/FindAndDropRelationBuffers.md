# FindAndDropRelationBuffers

## Location
src/backend/storage/buffer/bufmgr.c: 4315 - 4375

## Overview
FindAndDropRelationBuffers performs targeted lookup in the buffer mapping table to efficiently remove specific pages of a relation fork from the buffer pool.

## Definition
```c
static void FindAndDropRelationBuffers(RelFileLocator rlocator, ForkNumber forkNum, BlockNumber nForkBlock, BlockNumber firstDelBlock)
```

## Detailed Description
FindAndDropRelationBuffers implements an optimized approach to buffer removal by using the buffer mapping hash table for direct lookup rather than scanning the entire buffer pool. For each block number from firstDelBlock to nForkBlock-1, it creates a buffer tag, computes the hash, and looks up the buffer directly in the mapping table. If found, it locks the buffer header and double-checks the buffer identity before invalidating it. This approach is much more efficient than full buffer pool scans when the number of blocks to invalidate is relatively small.

## Parameters / Member Variables
- `rlocator`: Relation file locator identifying the specific relation
- `forkNum`: Fork number (main, FSM, VM, etc.) within the relation
- `nForkBlock`: Total number of blocks in the fork (upper bound for iteration)
- `firstDelBlock`: First block number to delete (blocks >= this value are removed)

## Dependencies
- Functions called/Symbols referenced:
  - InitBufferTag
  - BufTableHashCode
  - BufMappingPartitionLock
  - LWLockAcquire
  - BufTableLookup
  - LWLockRelease
  - GetBufferDescriptor
  - LockBufHdr
  - BufTagMatchesRelFileLocator
  - BufTagGetForkNum
  - InvalidateBuffer
  - UnlockBufHdr
  - BufferTag (type)
  - LWLock (type)
  - BufferDesc (type)
- Called from (representative examples):
  - DropRelationBuffers
  - DropRelationsAllBuffers

## Notes and Other Information
- Static function - only used internally within bufmgr.c
- More efficient than full buffer pool scans for small numbers of blocks
- Uses buffer mapping table hash lookup for O(1) average-case buffer finding
- Implements proper locking protocol: partition lock for lookup, buffer header lock for invalidation
- Double-checks buffer identity after acquiring buffer header lock to handle race conditions
- Critical optimization component used by both DropRelationBuffers and DropRelationsAllBuffers
- Requires exact relation size knowledge to iterate through valid block numbers