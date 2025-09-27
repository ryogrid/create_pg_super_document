# LockBufHdr

## Location
[src/backend/storage/buffer/bufmgr.c:5735-5764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5735-L5764)

## Overview
Acquires an exclusive lock on a buffer header by atomically setting the BM_LOCKED flag in the buffer state.

## Definition
```c
uint32 LockBufHdr(BufferDesc *desc)
```

## Detailed Description
This function implements a spinlock-style mechanism to acquire exclusive access to a buffer header. It uses atomic operations to set the BM_LOCKED flag in the buffer state, ensuring that only one process can hold the lock at a time. The function employs a spin-wait loop with exponential backoff delays to avoid busy-waiting inefficiently when the lock is contended. The lock is acquired by atomically setting the BM_LOCKED bit using pg_atomic_fetch_or_u32(), and if the bit was already set, the function spins with delays until it can successfully acquire the lock. This mechanism is essential for synchronizing access to buffer metadata during critical operations like buffer allocation, invalidation, and I/O.

## Parameters / Member Variables
- `desc`: Pointer to the BufferDesc structure whose header needs to be locked

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDesc](../B/BufferDesc.md) (structure type)
  - SpinDelayStatus (type for delay management)
  - BufferIsLocal (checks if buffer is local)
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md) (gets buffer number from descriptor)
  - init_local_spin_delay (initializes delay mechanism)
  - [pg_atomic_fetch_or_u32](../p/pg_atomic_fetch_or_u32.md) (atomic OR operation)
  - BM_LOCKED (buffer state flag)
  - [perform_spin_delay](../p/perform_spin_delay.md) (executes delay)
  - [finish_spin_delay](../f/finish_spin_delay.md) (cleans up delay state)
- Called from (representative examples):
  - [ReadRecentBuffer](../R/ReadRecentBuffer.md)
  - [BufferAlloc](../B/BufferAlloc.md)
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - [BufferSync](../B/BufferSync.md)
  - [FlushBuffer](../F/FlushBuffer.md)
  - [LockBufferForCleanup](LockBufferForCleanup.md)

## Notes and Other Information
- Only works on shared buffers (asserts that buffer is not local)
- Returns the old buffer state with BM_LOCKED bit set
- Uses spin-delay mechanism to avoid CPU waste during contention
- Critical for buffer management synchronization
- Must be paired with UnlockBufHdr() to release the lock
- Part of PostgreSQL low-level buffer synchronization infrastructure

## Simplified Source

```c
// Simplified version of LockBufHdr
uint32 LockBufHdr(BufferDesc *desc) {
    uint32 old_buf_state;

    // Ensure we're working with a shared buffer (not local)
    Assert(!BufferIsLocal(BufferDescriptorGetBuffer(desc)));

    // Initialize spin delay mechanism for contention handling
    SpinDelayStatus delayStatus;
    init_local_spin_delay(&delayStatus);

    // Spin until we successfully acquire the lock
    while (true) {
        // Atomically set the BM_LOCKED flag and get the old state
        old_buf_state = pg_atomic_fetch_or_u32(&desc->state, BM_LOCKED);

        // If the lock wasn't already held, we got it - break out
        if (!(old_buf_state & BM_LOCKED)) {
            break;
        }

        // Lock was contended - wait with exponential backoff
        perform_spin_delay(&delayStatus);
    }

    // Clean up delay state and return the new state
    finish_spin_delay(&delayStatus);
    return old_buf_state | BM_LOCKED;
}
```

Key simplifications made:
- Added descriptive comments explaining each major step
- Clarified the atomic operation and its purpose
- Explained the spin-wait mechanism with backoff
- Focused on the core lock acquisition algorithm
- Maintained the essential error checking and synchronization logic