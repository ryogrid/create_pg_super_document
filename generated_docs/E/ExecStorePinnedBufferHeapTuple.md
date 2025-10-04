# ExecStorePinnedBufferHeapTuple

## Location
[src/backend/executor/execTuples.c:1505-1532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1505-L1532)

## Overview
Stores an on-disk physical tuple from a buffer into a TTSOpsBufferHeapTuple slot, transferring buffer pin ownership from caller to slot.

## Definition

```c
TupleTableSlot *
ExecStorePinnedBufferHeapTuple(HeapTuple tuple,
							   TupleTableSlot *slot,
							   Buffer buffer)
```
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

## Simplified Source

```c
TupleTableSlot *
ExecStorePinnedBufferHeapTuple(HeapTuple tuple, TupleTableSlot *slot, Buffer buffer)
{
    // Validate inputs
    Assert(tuple != NULL);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);
    Assert(BufferIsValid(buffer));

    // Ensure correct slot type
    if (unlikely(!TTS_IS_BUFFERTUPLE(slot)))
        elog(ERROR, "trying to store an on-disk heap tuple into wrong type of slot");

    // Store tuple in buffer slot and transfer caller's pin
    tts_buffer_heap_store_tuple(slot, tuple, buffer, true);
    slot->tts_tableOid = tuple->t_tableOid;

    return slot;
}
```