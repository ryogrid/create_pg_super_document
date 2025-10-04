# tts_buffer_heap_store_tuple

## Location
[src/backend/executor/execTuples.c:942-1007](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L942-L1007)

## Overview
Stores a HeapTuple in a BufferHeapTupleTableSlot, managing buffer references and memory allocation to optimize performance during sequential scans.

## Definition

```c
static inline void
tts_buffer_heap_store_tuple(TupleTableSlot *slot, HeapTuple tuple,
							Buffer buffer, bool transfer_pin)
```
## Detailed Description
This function is a core component of PostgreSQL's tuple table slot mechanism, specifically designed for buffer-backed heap tuple table slots. It efficiently stores a HeapTuple in the slot while managing buffer pins to optimize memory usage and prevent unnecessary buffer reference counting operations.

The function implements an important optimization for sequential scans: when the new tuple resides on the same buffer page as the previously stored tuple, it avoids releasing and re-acquiring the buffer pin, which would be wasteful. This optimization significantly improves performance during table scans where consecutive tuples often reside on the same page.

The function handles memory management by freeing any previously materialized tuple if the TTS_SHOULDFREE flag is set, ensures proper buffer reference counting, and maintains the slot's metadata including the tuple identifier and validity flags.

## Parameters / Member Variables
- `*slot`: The TupleTableSlot to store the tuple in, must be a BufferHeapTupleTableSlot
- `tuple`: The HeapTuple to be stored in the slot
- `buffer`: The buffer containing the tuple's data page
- `transfer_pin`: If true, transfers ownership of the caller's buffer pin to the slot; if false, creates a new pin
## Dependencies
- Functions called/Symbols referenced:
  - [heap_freetuple](../h/heap_freetuple.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - [IncrBufferRefCount](../I/IncrBufferRefCount.md)
  - TTS_SHOULDFREE (macro)
  - [BufferIsValid](../B/BufferIsValid.md) (macro)
- Called from (representative examples):
  - [tts_buffer_heap_copyslot](tts_buffer_heap_copyslot.md)
  - [ExecStoreBufferHeapTuple](../E/ExecStoreBufferHeapTuple.md)
  - [ExecStorePinnedBufferHeapTuple](../E/ExecStorePinnedBufferHeapTuple.md)

## Notes and Other Information
- This is a static inline function optimized for performance in hot code paths
- Implements same-page optimization to avoid unnecessary buffer pin operations during sequential scans
- Handles both transfer_pin and non-transfer_pin modes for flexible buffer management
- Clears TTS_FLAG_EMPTY and resets tts_nvalid to indicate the slot now contains valid data
- The function assumes the caller already holds a pin on the buffer when transfer_pin is true

## Simplified Source

```c
static inline void
tts_buffer_heap_store_tuple(TupleTableSlot *slot, HeapTuple tuple,
                            Buffer buffer, bool transfer_pin)
{
    BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

    // Free any previously materialized tuple
    if (TTS_SHOULDFREE(slot)) {
        heap_freetuple(bslot->base.tuple);
        slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
    }

    // Initialize slot with new tuple
    slot->tts_flags &= ~TTS_FLAG_EMPTY;
    slot->tts_nvalid = 0;
    bslot->base.tuple = tuple;
    slot->tts_tid = tuple->t_self;

    // Optimize buffer pin management for same-page access
    if (bslot->buffer != buffer) {
        // Different buffer: release old, acquire new
        if (BufferIsValid(bslot->buffer))
            ReleaseBuffer(bslot->buffer);
        bslot->buffer = buffer;
        if (!transfer_pin && BufferIsValid(buffer))
            IncrBufferRefCount(buffer);
    } else if (transfer_pin && BufferIsValid(buffer)) {
        // Same buffer but transferring pin: release caller's pin
        ReleaseBuffer(buffer);
    }
}
```