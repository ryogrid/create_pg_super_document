# WaitBufHdrUnlocked

## Location
src/backend/storage/buffer/bufmgr.c: 5765 - 5788

## Overview
Waits until the BM_LOCKED flag is cleared from a buffer header and returns the buffer state at that point.

## Definition
```c
static uint32 WaitBufHdrUnlocked(BufferDesc *buf)
```

## Detailed Description
This function implements a spin-wait mechanism that blocks until a buffer header is unlocked (i.e., the BM_LOCKED flag is cleared from the buffer state). It continuously polls the buffer state using atomic read operations and uses a spin-delay mechanism to avoid excessive CPU consumption during the wait. The function is primarily designed for use in Compare-And-Swap (CAS) style loops where the caller needs to wait for a buffer to become unlocked before attempting further operations. Note that the buffer could be locked again by the time the function returns, so the returned state should be used immediately in atomic operations.

## Parameters / Member Variables
- `buf`: Pointer to the BufferDesc structure to wait for unlock

## Dependencies
- Functions called/Symbols referenced:
  - BufferDesc (structure type)
  - SpinDelayStatus (type for delay management)
  - init_local_spin_delay (initializes delay mechanism)
  - pg_atomic_read_u32 (atomic read operation)
  - BM_LOCKED (buffer state flag)
  - perform_spin_delay (executes delay)
  - finish_spin_delay (cleans up delay state)
- Called from (representative examples):
  - BufferIsPinned
  - MarkBufferDirty
  - PinBuffer
  - UnpinBufferNoOwner

## Notes and Other Information
- This is a static function internal to bufmgr.c
- Primarily useful in CAS (Compare-And-Swap) style loops
- The returned state may become stale immediately after return
- Uses spin-delay mechanism to reduce CPU waste during busy waiting
- Essential for buffer state synchronization in concurrent environments
- Complements LockBufHdr() by providing a way to wait for unlock completion
- Part of PostgreSQL low-level buffer synchronization infrastructure