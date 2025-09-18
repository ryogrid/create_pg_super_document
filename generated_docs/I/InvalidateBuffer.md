# InvalidateBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:1772-1869](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1772-L1869)

## Overview
InvalidateBuffer marks a shared buffer as invalid and returns it to the freelist, handling complex synchronization to ensure safe buffer reclamation during relation drops.

## Definition
```c
static void InvalidateBuffer(BufferDesc *buf)
```

## Detailed Description
InvalidateBuffer implements the critical logic for safely removing buffers from the buffer pool when they are no longer needed (such as during relation drops). The function must handle complex concurrency scenarios where other backends might be simultaneously accessing or writing the same buffer.

**Key Operations:**
1. **Tag Preservation**: Saves the original buffer tag before releasing locks to handle race conditions
2. **Concurrent Access Handling**: Waits for any ongoing I/O operations to complete before invalidation
3. **Hash Table Management**: Removes the buffer mapping from the lookup hash table
4. **State Cleanup**: Clears buffer tags, flags, and usage counts
5. **Freelist Management**: Returns the cleaned buffer to the replacement strategy's freelist

**Concurrency Safeguards:**
- Uses retry logic to handle buffers that change during lock acquisition
- Waits for pinned buffers (indicating ongoing writes) to be released
- Prevents invalidation of buffers pinned by the calling backend
- Maintains proper lock ordering to avoid deadlocks

The function is essential for PostgreSQL's ability to safely drop relations and reclaim buffer space.

## Parameters / Member Variables
- `buf`: Pointer to the BufferDesc structure to be invalidated. Must be locked (BM_LOCKED) upon entry.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - LockBufHdr
  - [BufTableHashCode](../B/BufTableHashCode.md)
  - [BufMappingPartitionLock](../B/BufMappingPartitionLock.md)
  - [BufferTagsEqual](../B/BufferTagsEqual.md)
  - BUF_STATE_GET_REFCOUNT
  - [GetPrivateRefCount](../G/GetPrivateRefCount.md)
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md)
  - WaitIO
  - [ClearBufferTag](../C/ClearBufferTag.md)
  - [BufTableDelete](../B/BufTableDelete.md)
  - StrategyFreeBuffer
- Constants used:
  - BM_LOCKED
  - LW_EXCLUSIVE
  - BUF_FLAG_MASK
  - BUF_USAGECOUNT_MASK
  - BM_TAG_VALID
- Called from (representative examples):
  - [DropRelationBuffers](../D/DropRelationBuffers.md)
  - [DropRelationsAllBuffers](../D/DropRelationsAllBuffers.md)
  - [FindAndDropRelationBuffers](../F/FindAndDropRelationBuffers.md)
  - [DropDatabaseBuffers](../D/DropDatabaseBuffers.md)

## Notes and Other Information
- Must be called with the buffer header spinlock held (BM_LOCKED state)
- The function drops the spinlock before returning as documented in the function comment
- Implements a retry mechanism to handle race conditions where the buffer tag changes during processing
- Includes safety checks to prevent invalidating buffers pinned by the calling backend
- The waiting logic for pinned buffers could theoretically loop indefinitely if reference counts are corrupted
- Critical for maintaining buffer pool consistency during DDL operations like DROP TABLE
- Only used in contexts where no other backend should be interested in the page content
- The function carefully coordinates between buffer locks, partition locks, and I/O completion
- Essential for PostgreSQL's transactional DDL implementation and storage space reclamation