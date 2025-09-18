# GetLocalBufferDescriptor

## Location
[src/include/storage/buf_internals.h:325-330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L325-L330)

## Overview
Returns a pointer to the local buffer descriptor structure for a given local buffer ID, providing access to metadata and control information for buffers used by temporary tables and local operations.

## Definition
```c
static inline BufferDesc *GetLocalBufferDescriptor(uint32 id)
```

## Detailed Description
This inline function provides efficient access to local buffer descriptors, which are used for managing buffers that are private to a single backend process (such as temporary tables). Unlike shared buffers accessed through GetBufferDescriptor, local buffers don't require complex locking mechanisms since they are only accessed by one process. The function returns a pointer to the BufferDesc structure from the LocalBufferDescriptors array at the specified index.

## Parameters / Member Variables
- `id`: A 32-bit unsigned integer representing the local buffer ID (index into the local buffer pool)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDesc](../B/BufferDesc.md) (structure type for buffer descriptors)
  - LocalBufferDescriptors (global array containing local buffer descriptors)
- Called from (representative examples):
  - [ReadRecentBuffer](../R/ReadRecentBuffer.md)
  - [ZeroAndLockBuffer](../Z/ZeroAndLockBuffer.md)
  - LocalBufferAlloc
  - GetLocalVictimBuffer
  - MarkLocalBufferDirty
  - DropRelationLocalBuffers
  - ExtendBufferedRelLocal

## Notes and Other Information
- This is an inline function for performance optimization, avoiding function call overhead
- Used specifically for local/temporary buffer management, separate from shared buffer pool
- Local buffers are simpler to manage since they don't require inter-process synchronization
- Part of PostgreSQL's dual buffer management system (shared vs local buffers)
- The function assumes the caller provides a valid local buffer ID within the allocated range
- Located in buf_internals.h as a core utility for local buffer operations
- Local buffers are typically used for temporary tables, sorts, and other backend-private operations