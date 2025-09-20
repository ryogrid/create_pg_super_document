# BufferDesc

## Location
[src/include/storage/buf_internals.h:245-256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L245-L256)

## Overview
The  structure is the core descriptor that contains metadata and state information for each buffer in PostgreSQL's shared buffer pool.

## Definition

```c
typedef struct BufferDesc
{
	BufferTag	tag;			/* ID of page contained in buffer */
	int			buf_id;			/* buffer's index number (from 0) */

	/* state of the tag, containing flags, refcount and usagecount */
	pg_atomic_uint32 state;

	int			wait_backend_pgprocno;	/* backend of pin-count waiter */
	int			freeNext;		/* link in freelist chain */
	LWLock		content_lock;	/* to lock access to buffer contents */
} BufferDesc;
```
## Detailed Description
The  structure serves as the shared descriptor and state data for each buffer in PostgreSQL's buffer management system. It contains all the necessary information to manage a single shared buffer, including its identity, state flags, reference counting, and synchronization mechanisms.

The structure is carefully designed to maintain performance while providing thread-safe access. The state field combines flags, reference count, and usage count into a single atomic variable, allowing many operations to be performed atomically without acquiring spinlocks. The structure size is kept under 64 bytes to fit within common CPU cache line sizes for optimal performance.

The same structure is used for both shared and local buffers, though some fields and locking mechanisms are not used for local buffers to reduce overhead.

## Parameters / Member Variables
- : BufferTag that uniquely identifies which disk block this buffer contains
- : The buffer's index number starting from 0, never changes after initialization
- : Atomic variable containing combined flags, reference count, and usage count for the buffer
- : Process ID of backend waiting for all pins on this buffer to be released
- : Link pointer for the freelist chain when buffer is unused
- : LWLock used to control access to the actual buffer contents (not the header)

## Dependencies
- Functions called/Symbols referenced:
  - BufferTag (for buffer identification)
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md) (for atomic state operations)
  - [LWLock](../L/LWLock.md) (for content locking)
- Called from (representative examples):
  - [BufferAlloc](BufferAlloc.md) (for buffer allocation)
  - PinBuffer (for buffer pinning operations)
  - UnpinBuffer (for buffer unpinning)
  - [FlushBuffer](../F/FlushBuffer.md) (for writing buffers to disk)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md) (for buffer access)
  - StrategyGetBuffer (for buffer strategy operations)

## Notes and Other Information
- Buffer header lock (BM_LOCKED flag in state) must be held to examine or change tag, state, or wait_backend_pgprocno fields
- The buffer header lock does NOT control access to buffer contents - that's handled by content_lock
- State updates without holding buffer header lock are restricted to Compare-And-Swap operations
- Structure size is kept below 64 bytes for CPU cache line optimization
- When a buffer is pinned, its tag cannot change, allowing tag examination without locking
- Only one backend can wait for pin count to reach zero per buffer at a time
- Used for both shared and local buffers with different locking semantics