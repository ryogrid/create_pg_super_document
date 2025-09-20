# BufMappingPartitionLockByIndex

## Location
[src/include/storage/buf_internals.h:193-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L193-L244)

## Overview
Returns a pointer to the lightweight lock (LWLock) for a specific buffer mapping partition identified by index, providing access to the lock that protects buffer mapping data structures.

## Definition

```c
struct for local buffer headers, but the locks are not
 * used and not all of the flag bits are useful either. To avoid unnecessary
 * overhead, manipulations of the state field should be done without actual
 * atomic operations (i.e. only pg_atomic_read_u32() and
 * pg_atomic_unlocked_write_u32()).
 *
 * Be careful to avoid increasing the size of the struct when adding or
 * reordering members.  Keeping it below 64 bytes (the most common CPU
 * cache line size) is fairly important for performance.
 *
 * Per-buffer I/O condition variables are currently kept outside this struct in
 * a separate array.  They could be moved in here and still fit within that
 * limit on common systems, but for now that is not done.
 */
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
This inline function provides efficient access to buffer mapping partition locks by index. It calculates the memory address of a specific LWLock within the MainLWLockArray by adding the provided index to the BUFFER_MAPPING_LWLOCK_OFFSET base offset. Buffer mapping partitions are used to reduce contention when multiple processes need to access the buffer mapping hash table simultaneously, with each partition having its own dedicated lock.

## Parameters / Member Variables
- : A 32-bit unsigned integer specifying which buffer mapping partition lock to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - BUFFER_MAPPING_LWLOCK_OFFSET (constant defining the base offset for buffer mapping locks)
  - MainLWLockArray (global array containing all lightweight locks)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- This is an inline function for performance optimization, avoiding function call overhead
- Part of PostgreSQL's buffer management subsystem that handles shared buffer access
- The function assumes the caller knows the valid range of partition indices
- Buffer mapping partitions help distribute lock contention across multiple locks rather than using a single global lock
- Located in buf_internals.h, indicating it's an internal buffer management utility function