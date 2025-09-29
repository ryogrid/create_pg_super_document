# FindAndDropRelationBuffers

## Location
[src/backend/storage/buffer/bufmgr.c:4315-4375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4315-L4375)

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
  - [InitBufferTag](../I/InitBufferTag.md)
  - [BufTableHashCode](../B/BufTableHashCode.md)
  - [BufMappingPartitionLock](../B/BufMappingPartitionLock.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [BufTableLookup](../B/BufTableLookup.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [BufTagMatchesRelFileLocator](../B/BufTagMatchesRelFileLocator.md)
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - BufferTag (type)
  - [LWLock](../L/LWLock.md) (type)
  - [BufferDesc](../B/BufferDesc.md) (type)
- Called from (representative examples):
  - [DropRelationBuffers](../D/DropRelationBuffers.md)
  - [DropRelationsAllBuffers](../D/DropRelationsAllBuffers.md)

## Notes and Other Information
- Static function - only used internally within bufmgr.c
- More efficient than full buffer pool scans for small numbers of blocks
- Uses buffer mapping table hash lookup for O(1) average-case buffer finding
- Implements proper locking protocol: partition lock for lookup, buffer header lock for invalidation
- Double-checks buffer identity after acquiring buffer header lock to handle race conditions
- Critical optimization component used by both DropRelationBuffers and DropRelationsAllBuffers
- Requires exact relation size knowledge to iterate through valid block numbers

## Simplified Source

```c
static void FindAndDropRelationBuffers(RelFileLocator rlocator, ForkNumber forkNum,
                                      BlockNumber nForkBlock, BlockNumber firstDelBlock) {
    // Iterate through each block to be dropped
    for (BlockNumber curBlock = firstDelBlock; curBlock < nForkBlock; curBlock++) {
        BufferTag bufTag;
        uint32 bufHash;
        LWLock *bufPartitionLock;
        int buf_id;

        // Create buffer tag for this specific block
        InitBufferTag(&bufTag, &rlocator, forkNum, curBlock);

        // Calculate hash and get partition lock
        bufHash = BufTableHashCode(&bufTag);
        bufPartitionLock = BufMappingPartitionLock(bufHash);

        // Look up buffer in mapping table
        LWLockAcquire(bufPartitionLock, LW_SHARED);
        buf_id = BufTableLookup(&bufTag, bufHash);
        LWLockRelease(bufPartitionLock);

        // Skip if buffer not found in pool
        if (buf_id < 0)
            continue;

        // Get buffer descriptor and lock it
        BufferDesc *bufHdr = GetBufferDescriptor(buf_id);
        uint32 buf_state = LockBufHdr(bufHdr);

        // Double-check buffer identity and invalidate if match
        if (BufTagMatchesRelFileLocator(&bufHdr->tag, &rlocator) &&
            BufTagGetForkNum(&bufHdr->tag) == forkNum &&
            bufHdr->tag.blockNum >= firstDelBlock) {
            // InvalidateBuffer releases the spinlock
            InvalidateBuffer(bufHdr);
        } else {
            // Not our buffer, just unlock
            UnlockBufHdr(bufHdr, buf_state);
        }
    }
}
```