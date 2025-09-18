# ExecStorePinnedBufferHeapTuple

## Location
src/backend/executor/execTuples.c: 1505 - 1532

## Overview
Stores an on-disk physical tuple from a buffer into a TTSOpsBufferHeapTuple slot, transferring buffer pin ownership from caller to slot.

## Definition


## Detailed Description
ExecStorePinnedBufferHeapTuple is similar to ExecStoreBufferHeapTuple but with a key difference in buffer pin management. This function transfers an existing buffer pin from the caller to the tuple slot, meaning the caller does not need to (and may not) release the pin themselves. The slot assumes full responsibility for managing the buffer pin lifecycle.

This function is particularly useful in scenarios where the caller already holds a buffer pin and wants to transfer ownership to the slot for efficient resource management. The pin transfer mechanism prevents double-pinning and ensures proper cleanup when the slot is eventually cleared.

## Parameters
- : HeapTuple to store (pointing to data in the pinned buffer)
- : TupleTableSlot of TTSOpsBufferHeapTuple type to store the tuple in
- : Already-pinned disk buffer containing the tuple data

## Dependencies
- Functions called/Symbols referenced:
  - [BufferIsValid](../B/BufferIsValid.md)
  - TTS_IS_BUFFERTUPLE
  - [tts_buffer_heap_store_tuple](../t/tts_buffer_heap_store_tuple.md) (called with pin_transfer=true)

- Called from (representative examples):
  - [heapam_fetch_row_version](../h/heapam_fetch_row_version.md)
  - [heapam_tuple_lock](../h/heapam_tuple_lock.md)

## Notes and Other Information
- The key difference from ExecStoreBufferHeapTuple is pin ownership transfer (pin_transfer=true)
- Caller must NOT release the buffer pin after calling this function
- The slot takes full responsibility for pin management and cleanup
- Requires the buffer to be already pinned by the caller
- Used primarily in heap access method implementations for row locking and version fetching
- More efficient than ExecStoreBufferHeapTuple when the caller already has a pin
- Part of PostgreSQL's buffer pin optimization strategy for tuple access
- Returns the passed-in slot pointer for convenience