# LockBufHdr

## Location
src/backend/storage/buffer/bufmgr.c: 5735 - 5764

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
  - BufferDesc (structure type)
  - SpinDelayStatus (type for delay management)
  - BufferIsLocal (checks if buffer is local)
  - BufferDescriptorGetBuffer (gets buffer number from descriptor)
  - init_local_spin_delay (initializes delay mechanism)
  - pg_atomic_fetch_or_u32 (atomic OR operation)
  - BM_LOCKED (buffer state flag)
  - perform_spin_delay (executes delay)
  - finish_spin_delay (cleans up delay state)
- Called from (representative examples):
  - ReadRecentBuffer
  - BufferAlloc
  - InvalidateBuffer
  - GetVictimBuffer
  - BufferSync
  - FlushBuffer
  - LockBufferForCleanup

## Notes and Other Information
- Only works on shared buffers (asserts that buffer is not local)
- Returns the old buffer state with BM_LOCKED bit set
- Uses spin-delay mechanism to avoid CPU waste during contention
- Critical for buffer management synchronization
- Must be paired with UnlockBufHdr() to release the lock
- Part of PostgreSQL low-level buffer synchronization infrastructure