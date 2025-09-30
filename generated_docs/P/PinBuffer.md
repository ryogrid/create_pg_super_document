# PinBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:2641-2751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2641-L2751)

## Overview
PinBuffer makes a shared buffer unavailable for replacement by incrementing its reference count and managing its usage count based on the access strategy to prevent eviction during active use.

## Definition
```c
static bool PinBuffer(BufferDesc *buf, BufferAccessStrategy strategy)
```

## Detailed Description
This internal function pins a shared buffer to prevent it from being selected for replacement by the buffer manager. It uses lock-free atomic operations with compare-and-swap loops to efficiently update the buffer state without acquiring the buffer header lock, which is crucial for performance given the high frequency of buffer pinning operations.

The function manages two key aspects: reference counting (to track how many processes are using the buffer) and usage counting (for the clock-sweep replacement algorithm). The usage count behavior differs based on the access strategy - the default strategy increments usage count up to the maximum, while ring buffer strategies limit usage count to 1 to prevent interference with other access patterns.

The function maintains a private reference count entry per backend to track local pins and handles both new pins (requiring atomic state updates) and repeat pins (simply incrementing the local reference count).

## Parameters / Member Variables
- `buf`: Buffer descriptor to pin
- `strategy`: Buffer access strategy determining usage count behavior (NULL for default strategy)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md): Converts buffer descriptor to Buffer ID
  - BufferIsLocal: Assertion to verify this is a shared buffer
  - [GetPrivateRefCountEntry](../G/GetPrivateRefCountEntry.md): Gets or creates private reference count entry
  - [NewPrivateRefCountEntry](../N/NewPrivateRefCountEntry.md): Creates new private reference count entry
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md): Atomic read of buffer state
  - [WaitBufHdrUnlocked](../W/WaitBufHdrUnlocked.md): Waits for buffer header to be unlocked
  - [pg_atomic_compare_exchange_u32](../p/pg_atomic_compare_exchange_u32.md): Atomic compare-and-swap operation
  - BufHdrGetBlock: Gets block data for Valgrind instrumentation
  - [ResourceOwnerRememberBuffer](../R/ResourceOwnerRememberBuffer.md): Tracks buffer ownership for cleanup
  - BM_LOCKED: Buffer state flag for locked status
  - BM_VALID: Buffer state flag for valid data
  - BUF_REFCOUNT_ONE: Constant for incrementing reference count
  - BUF_USAGECOUNT_ONE: Constant for incrementing usage count
  - BUF_STATE_GET_USAGECOUNT: Extracts usage count from buffer state
  - BM_MAX_USAGE_COUNT: Maximum allowed usage count
  - VALGRIND_MAKE_MEM_DEFINED: Valgrind memory debugging support
- Called from (representative examples):
  - BufferIsPinned: Buffer status checking
  - [ReadRecentBuffer](../R/ReadRecentBuffer.md): Reading recently accessed buffers
  - [BufferAlloc](../B/BufferAlloc.md): Buffer allocation and reuse
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md): Extending relations with shared buffers

## Notes and Other Information
- Returns true if buffer contains valid data (BM_VALID flag set), false otherwise
- Uses lock-free atomic operations for high performance in concurrent environments
- Requires prior calls to ResourceOwnerEnlarge() and ReservePrivateRefCountEntry()
- Different usage count strategies: default increments up to max, ring buffers limit to 1
- Maintains per-backend private reference counts for efficient local tracking
- Integrates with Valgrind for memory debugging in development builds
- The function is static (internal to bufmgr.c) and not directly callable by external code
- Handles race conditions through atomic compare-and-swap retry loops
- Access strategy affects replacement behavior: NULL strategy allows full usage count, non-NULL limits interference with other backends

## Simplified Source

```c
static bool PinBuffer(BufferDesc *buf, BufferAccessStrategy strategy)
{
    Buffer b = BufferDescriptorGetBuffer(buf);
    bool result;
    PrivateRefCountEntry *ref;

    Assert(!BufferIsLocal(b));
    Assert(ReservedRefCountEntry != NULL);

    // Get or create private reference count entry for this buffer
    ref = GetPrivateRefCountEntry(b, true);

    if (ref == NULL) {
        // First time pinning this buffer - need to update shared state
        uint32 buf_state;
        uint32 old_buf_state;

        ref = NewPrivateRefCountEntry(b);

        // Atomic loop to update buffer state (refcount + usage count)
        old_buf_state = pg_atomic_read_u32(&buf->state);
        for (;;) {
            // Wait if buffer is locked
            if (old_buf_state & BM_LOCKED)
                old_buf_state = WaitBufHdrUnlocked(buf);

            buf_state = old_buf_state;

            // Increment reference count
            buf_state += BUF_REFCOUNT_ONE;

            // Update usage count based on strategy
            if (strategy == NULL) {
                // Default: increment usage count up to maximum
                if (BUF_STATE_GET_USAGECOUNT(buf_state) < BM_MAX_USAGE_COUNT)
                    buf_state += BUF_USAGECOUNT_ONE;
            } else {
                // Ring buffer: only set usage count to 1 if it was 0
                if (BUF_STATE_GET_USAGECOUNT(buf_state) == 0)
                    buf_state += BUF_USAGECOUNT_ONE;
            }

            // Try to atomically update the state
            if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state, buf_state)) {
                result = (buf_state & BM_VALID) != 0;
                break;
            }
            // If CAS failed, retry with new old_buf_state
        }
    } else {
        // Buffer already pinned by us - just check if it's valid
        result = (pg_atomic_read_u32(&buf->state) & BM_VALID) != 0;
    }

    // Increment local reference count and register with resource owner
    ref->refcount++;
    Assert(ref->refcount > 0);
    ResourceOwnerRememberBuffer(CurrentResourceOwner, b);

    return result;
}
```