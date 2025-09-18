# GetBufferDescriptor

## Location
[src/include/storage/buf_internals.h:319-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L319-L324)

## Overview
Returns a pointer to the buffer descriptor structure for a given buffer ID, providing access to the metadata and control information for a specific shared buffer.

## Definition
```c
static inline BufferDesc *GetBufferDescriptor(uint32 id)
```

## Detailed Description
This inline function provides efficient access to buffer descriptors by converting a buffer ID into a pointer to the corresponding BufferDesc structure. Buffer descriptors contain metadata about shared buffers including the buffer tag, reference count, usage count, I/O state, and other control information. The function accesses the BufferDescriptors global array and returns the bufferdesc field of the specified entry.

## Parameters / Member Variables
- `id`: A 32-bit unsigned integer representing the buffer ID (index into the buffer pool)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDesc](../B/BufferDesc.md) (structure type for buffer descriptors)
  - BufferDescriptors (global array containing buffer descriptor entries)
- Called from (representative examples):
  - InitBufferPool
  - [ReadRecentBuffer](../R/ReadRecentBuffer.md)
  - [ZeroAndLockBuffer](../Z/ZeroAndLockBuffer.md)
  - [BufferAlloc](../B/BufferAlloc.md)
  - MarkBufferDirty
  - ReleaseBuffer
  - [LockBuffer](../L/LockBuffer.md)
  - And many other buffer management functions

## Notes and Other Information
- This is an inline function for performance optimization, avoiding function call overhead
- Central function in PostgreSQL's buffer management system, used extensively throughout the codebase
- The function assumes the caller provides a valid buffer ID within the range of allocated buffers
- Part of the shared buffer pool infrastructure that manages pages cached in memory
- Returns a direct pointer to the buffer descriptor, allowing callers to access and modify buffer metadata
- Located in buf_internals.h as a core utility function for internal buffer operations