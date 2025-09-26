# FlushDatabaseBuffers

## Location
[src/backend/storage/buffer/bufmgr.c:4835-4876](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4835-L4876)

## Overview
FlushDatabaseBuffers writes all dirty pages of a specific database to disk, ensuring the kernel has an up-to-date view of the database by flushing all modified buffers.

## Definition
```c
void FlushDatabaseBuffers(Oid dbid)
```

## Detailed Description
This function performs a comprehensive flush of all dirty buffers belonging to a specific database. It iterates through the entire buffer pool to identify and flush dirty pages that belong to the target database. The operation includes:

- Scanning all buffers in the shared buffer pool (NBuffers)
- Performing an unlocked precheck for efficiency before acquiring locks
- Properly pinning buffers and acquiring content locks before flushing
- Using FlushBuffer to perform the actual write operation
- Ensuring proper resource management through pins and lock acquisition

The function is designed for situations where a complete database flush is required, such as during database operations that need to ensure all changes are written to disk.

## Parameters / Member Variables
- `dbid`: OID of the database whose buffers should be flushed to disk

## Dependencies
- Functions called/Symbols referenced:
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [ReservePrivateRefCountEntry](../R/ReservePrivateRefCountEntry.md)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)
  - [LockBufHdr](../L/LockBufHdr.md), UnlockBufHdr
  - [PinBuffer_Locked](../P/PinBuffer_Locked.md), UnpinBuffer
  - [LWLockAcquire](../L/LWLockAcquire.md), LWLockRelease
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md)
  - [FlushBuffer](FlushBuffer.md)
  - BM_VALID, BM_DIRTY flags
  - IOOBJECT_RELATION, IOCONTEXT_NORMAL constants
- Called from (representative examples):
  - [dbase_redo](../d/dbase_redo.md)

## Notes and Other Information
- The function assumes the caller holds appropriate locks to prevent other backends from dirtying pages in the target database
- Uses an unlocked precheck optimization similar to DropRelationBuffers to avoid unnecessary work
- Temporary relation pages are intentionally ignored as they are not considered interesting for this operation
- Proper resource management is ensured through ReservePrivateRefCountEntry and ResourceOwnerEnlarge calls
- Content locks are acquired in shared mode since the buffer content is only being read for flushing
- The function handles the case where buffer tags might change between the precheck and the locked check