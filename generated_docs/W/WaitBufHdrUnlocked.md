# WaitBufHdrUnlocked

## Location
[src/backend/storage/buffer/bufmgr.c:5765-5788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5765-L5788)

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
  - [BufferDesc](../B/BufferDesc.md) (structure type)
  - SpinDelayStatus (type for delay management)
  - init_local_spin_delay (initializes delay mechanism)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md) (atomic read operation)
  - BM_LOCKED (buffer state flag)
  - [perform_spin_delay](../p/perform_spin_delay.md) (executes delay)
  - [finish_spin_delay](../f/finish_spin_delay.md) (cleans up delay state)
- Called from (representative examples):
  - BufferIsPinned
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [PinBuffer](../P/PinBuffer.md)
  - [UnpinBufferNoOwner](../U/UnpinBufferNoOwner.md)

## Notes and Other Information
- This is a static function internal to bufmgr.c
- Primarily useful in CAS (Compare-And-Swap) style loops
- The returned state may become stale immediately after return
- Uses spin-delay mechanism to reduce CPU waste during busy waiting
- Essential for buffer state synchronization in concurrent environments
- Complements LockBufHdr() by providing a way to wait for unlock completion
- Part of PostgreSQL low-level buffer synchronization infrastructure

## Simplified Source

```c
static uint32
WaitBufHdrUnlocked(BufferDesc *buf)
{
    SpinDelayStatus delayStatus;
    uint32 buf_state;

    // Initialize spin delay mechanism to avoid excessive CPU usage
    init_local_spin_delay(&delayStatus);

    // Read the current buffer state
    buf_state = pg_atomic_read_u32(&buf->state);

    // Wait until the BM_LOCKED flag is cleared
    while (buf_state & BM_LOCKED)
    {
        // Use spin delay to reduce CPU consumption
        perform_spin_delay(&delayStatus);
        buf_state = pg_atomic_read_u32(&buf->state);
    }

    // Clean up delay mechanism
    finish_spin_delay(&delayStatus);

    return buf_state;
}
```