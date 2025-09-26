# BufferDescriptorGetContentLock

## Location
[src/include/storage/buf_internals.h:343-351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L343-L351)

## Overview
Returns a pointer to the content lock associated with a buffer descriptor, providing access to the lightweight lock that protects the buffer's data content.

## Definition

```c
static inline LWLock *
BufferDescriptorGetContentLock(const BufferDesc *bdesc)
```
## Detailed Description
BufferDescriptorGetContentLock is a static inline function that provides access to the content lock embedded within a buffer descriptor. This function serves as a type-safe accessor to retrieve the LWLock that protects the actual data content of a buffer page. The content lock is used to coordinate access to the buffer's page data, ensuring that only one process can modify the page content at a time while allowing multiple readers when appropriate.

The function performs a simple cast operation to return the address of the content_lock field within the BufferDesc structure as an LWLock pointer. This design allows the buffer management system to treat the embedded lock as a standard LWLock for all locking operations.

## Parameters / Member Variables
- : Pointer to a BufferDesc structure representing the buffer descriptor whose content lock is to be accessed

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDesc](BufferDesc.md) (structure type)
  - [LWLock](../L/LWLock.md) (structure type)
- Called from (representative examples):
  - [InitBufferPool](../I/InitBufferPool.md)
  - [ZeroAndLockBuffer](../Z/ZeroAndLockBuffer.md)
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [ConditionalLockBuffer](../C/ConditionalLockBuffer.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [FlushOneBuffer](../F/FlushOneBuffer.md)

## Notes and Other Information
- This is a static inline function defined in buf_internals.h, providing efficient access with no function call overhead
- The function is widely used throughout the buffer management subsystem (bufmgr.c) for acquiring content locks
- The content lock is distinct from the buffer header lock and protects the actual page data rather than the buffer metadata
- This function is essential for the buffer locking protocol in PostgreSQL's shared buffer pool management