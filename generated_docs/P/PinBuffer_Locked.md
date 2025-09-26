# PinBuffer_Locked

## Location
[src/backend/storage/buffer/bufmgr.c:2752-2794](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2752-L2794)

## Overview
PinBuffer_Locked pins a buffer while the caller already holds the buffer header spinlock, providing an optimized path for pinning when the lock is already acquired and no preexisting pin exists.

## Definition
```c
static void PinBuffer_Locked(BufferDesc *buf)
```

## Detailed Description
This specialized function pins a buffer when the caller already holds the buffer header spinlock. It is designed for scenarios where the buffer is known to not have any preexisting pins by the current backend, allowing it to skip the expensive private reference count array and hash table searches that would normally be required.

The function performs the pinning operation in one atomic step: it increments the buffer's reference count and releases the spinlock simultaneously using UnlockBufHdr(). This approach is frequently mandatory rather than just an optimization, because it ensures the buffer state cannot change between the pin operation and the spinlock release.

Unlike the general PinBuffer() function, this variant does not modify the buffer's usage count and does not perform BM_VALID checks, leaving such concerns to the caller. It creates a new private reference count entry since no preexisting pin is expected.

## Parameters / Member Variables
- `buf`: Buffer descriptor to pin (caller must already hold its spinlock)

## Dependencies
- Functions called/Symbols referenced:
  - GetPrivateRefCountEntry: Assertion to verify no preexisting pin exists
  - BufferDescriptorGetBuffer: Converts buffer descriptor to Buffer ID
  - BufHdrGetBlock: Gets block data for Valgrind instrumentation
  - VALGRIND_MAKE_MEM_DEFINED: Valgrind memory debugging support
  - pg_atomic_read_u32: Atomic read of buffer state
  - UnlockBufHdr: Atomically updates state and releases spinlock
  - NewPrivateRefCountEntry: Creates new private reference count entry
  - ResourceOwnerRememberBuffer: Tracks buffer ownership for cleanup
  - BM_LOCKED: Buffer state flag for locked status
  - BUF_REFCOUNT_ONE: Constant for incrementing reference count
- Called from (representative examples):
  - BufferIsPinned: Buffer status checking operations
  - ReadRecentBuffer: Recent buffer access optimization
  - GetVictimBuffer: Buffer replacement victim selection
  - SyncOneBuffer: Individual buffer synchronization
  - FlushRelationBuffers: Relation-specific buffer flushing
  - FlushRelationsAllBuffers: Multi-relation buffer flushing
  - FlushDatabaseBuffers: Database-wide buffer flushing
  - EvictUnpinnedBuffer: Buffer eviction operations

## Notes and Other Information
- Requires caller to already hold the buffer header spinlock
- Assumes no preexisting pin by the current backend exists
- Must call ReservePrivateRefCountEntry() and ResourceOwnerEnlarge() before use
- Does not modify buffer usage count unlike PinBuffer()
- Does not perform BM_VALID checks - caller's responsibility
- Uses optimized path that skips private reference count array searches
- Releases the spinlock as part of the pin operation for atomicity
- Often mandatory rather than optional to prevent race conditions
- The function is static (internal to bufmgr.c) and not directly callable by external code
- Integrates with Valgrind for memory debugging support
- Designed for high-performance scenarios where lock is already held