# PinLocalBuffer

## Location
[src/backend/storage/buffer/localbuf.c:655-680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L655-L680)

## Overview
Pins a local buffer to prevent it from being evicted, incrementing its reference count and optionally adjusting its usage count.

## Definition
bool PinLocalBuffer(BufferDesc *buf_hdr, bool adjust_usagecount)

## Detailed Description
This function pins a local buffer by incrementing its reference count, ensuring it cannot be evicted from the buffer cache while pinned. When a buffer's reference count increases from 0 to 1, it increments the global count of pinned local buffers. The function can optionally adjust the buffer's usage count to influence its position in the buffer replacement algorithm.

The function also registers the buffer with the current resource owner for automatic cleanup if the transaction aborts. It uses atomic operations to safely read the buffer state and returns whether the buffer contains valid data, which helps callers determine if they need to read the page from disk.

## Parameters / Member Variables
- `buf_hdr`: Pointer to the BufferDesc structure representing the local buffer to pin
- `adjust_usagecount`: Boolean flag indicating whether to increment the buffer's usage count for LRU algorithm

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - BUF_STATE_GET_USAGECOUNT
  - [pg_atomic_unlocked_write_u32](../p/pg_atomic_unlocked_write_u32.md)
  - [ResourceOwnerRememberBuffer](../R/ResourceOwnerRememberBuffer.md)
- Called from (representative examples):
  - [ReadRecentBuffer](../R/ReadRecentBuffer.md)
  - [LocalBufferAlloc](../L/LocalBufferAlloc.md)
  - [GetLocalVictimBuffer](../G/GetLocalVictimBuffer.md)
  - [ExtendBufferedRelLocal](../E/ExtendBufferedRelLocal.md)
  - [ResourceOwnerForgetBufferIO](../R/ResourceOwnerForgetBufferIO.md)

## Notes and Other Information
- Returns true if buffer contains valid data (BM_VALID flag set), false otherwise
- Increments NLocalPinnedBuffers counter when reference count goes from 0 to 1
- Usage count adjustment helps with buffer replacement policy - frequently used buffers stay cached longer
- Registers buffer with resource owner for automatic cleanup on transaction abort
- Must be called after ResourceOwnerEnlarge() to ensure resource tracking capacity
- Uses atomic operations for safe concurrent access to buffer state
- Buffer ID calculation: bufid = -buffer - 1 (converts negative buffer ID to array index)
- Could be optimized for cases where usage count adjustment isn't needed, but current unified approach is preferred

## Simplified Source

```c
bool PinLocalBuffer(BufferDesc *buf_hdr, bool adjust_usagecount)
{
    uint32 buf_state;
    Buffer buffer = BufferDescriptorGetBuffer(buf_hdr);
    int bufid = -buffer - 1;  // Convert buffer ID to array index

    // Read buffer state atomically
    buf_state = pg_atomic_read_u32(&buf_hdr->state);

    // If this is the first pin on this buffer
    if (LocalRefCount[bufid] == 0) {
        NLocalPinnedBuffers++;  // Increment global pinned buffer count

        // Optionally increment usage count for LRU algorithm
        if (adjust_usagecount &&
            BUF_STATE_GET_USAGECOUNT(buf_state) < BM_MAX_USAGE_COUNT) {
            buf_state += BUF_USAGECOUNT_ONE;
            pg_atomic_unlocked_write_u32(&buf_hdr->state, buf_state);
        }
    }

    // Increment this buffer's reference count
    LocalRefCount[bufid]++;

    // Register with resource owner for automatic cleanup
    ResourceOwnerRememberBuffer(CurrentResourceOwner, BufferDescriptorGetBuffer(buf_hdr));

    // Return whether buffer contains valid data
    return buf_state & BM_VALID;
}
```