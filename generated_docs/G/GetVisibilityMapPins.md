# GetVisibilityMapPins

## Location
[src/backend/access/heap/hio.c:140-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/hio.c#L140-L237)

## Overview
GetVisibilityMapPins manages visibility map pins for heap pages that are all-visible, ensuring proper locking order and handling the complex coordination between buffer locks and visibility map pins.

## Definition

```c
static bool
GetVisibilityMapPins(Relation relation, Buffer buffer1, Buffer buffer2,
					 BlockNumber block1, BlockNumber block2,
					 Buffer *vmbuffer1, Buffer *vmbuffer2)
```
## Detailed Description
This function manages the acquisition of visibility map pins for heap pages that are marked as all-visible. It coordinates between potentially two buffers and their corresponding visibility map pages, handling complex locking scenarios:

- Normalizes buffer order to handle single buffer cases and maintain proper lock ordering (block1 <= block2)
- Checks if pages are all-visible and whether visibility map pins are needed
- Implements careful lock management: releases buffer locks before I/O operations to avoid deadlocks
- Acquires visibility map pins through visibilitymap_pin() for all-visible pages
- Re-acquires buffer locks after pin operations
- Handles race conditions where pages may become all-visible during the pin process
- Returns whether buffer locks were temporarily released, allowing callers to handle potential state changes

The function includes a retry loop to handle cases where visibility status changes during pin acquisition.

## Parameters / Member Variables
- `relation`: The relation containing the heap pages
- `buffer1`: First buffer that may need visibility map pinning
- `buffer2`: Second buffer that may need visibility map pinning (may be InvalidBuffer)
- `block1`: Block number corresponding to buffer1
- `block2`: Block number corresponding to buffer2 (may be smaller than block1)
- `*vmbuffer1`: Output parameter for visibility map buffer corresponding to block1
- `*vmbuffer2`: Output parameter for visibility map buffer corresponding to block2
## Dependencies
- Functions called/Symbols referenced:
  - [BufferIsValid](../B/BufferIsValid.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageIsAllVisible](../P/PageIsAllVisible.md)
  - [visibilitymap_pin_ok](../v/visibilitymap_pin_ok.md)
  - [visibilitymap_pin](../v/visibilitymap_pin.md)
  - [LockBuffer](../L/LockBuffer.md)
- Constants referenced:
  - InvalidBuffer
  - BUFFER_LOCK_UNLOCK
  - BUFFER_LOCK_EXCLUSIVE
- Called from:
  - [RelationGetBufferForTuple](../R/RelationGetBufferForTuple.md) (multiple locations)

## Notes and Other Information
- Static function within hio.c, designed specifically for heap I/O operations
- Implements sophisticated buffer ordering logic to prevent deadlocks
- The function's return value (released_locks) is critical for callers to know if they need to revalidate page states
- Handles edge cases like single buffer operations and overlapping buffer scenarios
- The retry loop prevents race conditions where page visibility status changes during pin acquisition
- Buffer lock release during I/O is essential to prevent deadlocks with visibility map operations

## Simplified Source
```c
static bool GetVisibilityMapPins(Relation relation, Buffer buffer1, Buffer buffer2,
                                BlockNumber block1, BlockNumber block2,
                                Buffer *vmbuffer1, Buffer *vmbuffer2) {
    bool released_locks = false;

    // Normalize buffer order: ensure buffer1 is valid and block1 <= block2
    if (!BufferIsValid(buffer1) || (BufferIsValid(buffer2) && block1 > block2)) {
        // Swap buffers and block numbers
        swap_buffers_and_blocks();
    }

    while (1) {
        // Check which visibility map pins we need
        bool need_pin1 = PageIsAllVisible(BufferGetPage(buffer1)) &&
                        !visibilitymap_pin_ok(block1, *vmbuffer1);
        bool need_pin2 = buffer2 != InvalidBuffer &&
                        PageIsAllVisible(BufferGetPage(buffer2)) &&
                        !visibilitymap_pin_ok(block2, *vmbuffer2);

        if (!need_pin1 && !need_pin2)
            break; // All pins are ready

        // Release buffer locks before I/O to avoid deadlocks
        released_locks = true;
        LockBuffer(buffer1, BUFFER_LOCK_UNLOCK);
        if (buffer2 != InvalidBuffer && buffer2 != buffer1)
            LockBuffer(buffer2, BUFFER_LOCK_UNLOCK);

        // Acquire needed visibility map pins
        if (need_pin1)
            visibilitymap_pin(relation, block1, vmbuffer1);
        if (need_pin2)
            visibilitymap_pin(relation, block2, vmbuffer2);

        // Re-acquire buffer locks
        LockBuffer(buffer1, BUFFER_LOCK_EXCLUSIVE);
        if (buffer2 != InvalidBuffer && buffer2 != buffer1)
            LockBuffer(buffer2, BUFFER_LOCK_EXCLUSIVE);

        // Check if we need another iteration for race conditions
        if (single_buffer_or_both_pinned())
            break;
    }

    return released_locks;
}
```