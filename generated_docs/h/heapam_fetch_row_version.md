# heapam_fetch_row_version

## Location
[src/backend/access/heap/heapam_handler.c:181-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L181-L204)

## Overview
This function retrieves a specific tuple from a heap relation using its tuple identifier (TID) and stores it in a table slot, serving as a callback for non-modifying operations on individual tuples in the heap access method.

## Definition

```c
static bool
heapam_fetch_row_version(Relation relation,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot)
```
## Detailed Description
heapam_fetch_row_version is a static callback function used by the heap access method to fetch a specific tuple version from a heap relation. It acts as a wrapper around the lower-level heap_fetch function, providing the interface expected by the table access method layer. The function attempts to retrieve the tuple identified by the given TID, checks its visibility according to the provided snapshot, and if successful, stores the tuple in the provided table slot with proper buffer management.

The function ensures that the slot is of the correct type (BufferHeapTupleTableSlot) and handles the buffer pin transfer from the heap_fetch operation to the slot, maintaining proper resource management.

## Parameters / Member Variables
- : The heap relation from which to fetch the tuple
- : ItemPointer containing the tuple identifier (block number and offset) of the target tuple
- : Snapshot used for visibility checking to determine if the tuple version is visible to the current transaction
- : TupleTableSlot where the fetched tuple will be stored (must be a BufferHeapTupleTableSlot)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferHeapTupleTableSlot](../B/BufferHeapTupleTableSlot.md) (type cast)
  - TTS_IS_BUFFERTUPLE (assertion check)
  - [heap_fetch](heap_fetch.md) (core tuple fetching)
  - [ExecStorePinnedBufferHeapTuple](../E/ExecStorePinnedBufferHeapTuple.md) (slot storage)
  - RelationGetRelid (relation OID retrieval)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)

## Notes and Other Information
- This is a static function serving as a callback in the table access method interface
- The function assumes the slot parameter is of type BufferHeapTupleTableSlot and asserts this with TTS_IS_BUFFERTUPLE
- On successful fetch, the function transfers the buffer pin from heap_fetch to the slot, ensuring proper buffer reference management
- Returns true if the tuple was successfully fetched and is visible according to the snapshot, false otherwise
- Part of the heap access method handler callbacks for non-modifying tuple operations

## Simplified Source

```c
static bool heapam_fetch_row_version(Relation relation,
                                   ItemPointer tid,
                                   Snapshot snapshot,
                                   TupleTableSlot *slot) {
    BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
    Buffer buffer;

    Assert(TTS_IS_BUFFERTUPLE(slot));

    // Set the TID in the tuple data
    bslot->base.tupdata.t_self = *tid;

    // Fetch the tuple from heap storage
    if (heap_fetch(relation, snapshot, &bslot->base.tupdata, &buffer, false)) {
        // Store in slot, transferring buffer pin ownership
        ExecStorePinnedBufferHeapTuple(&bslot->base.tupdata, slot, buffer);
        slot->tts_tableOid = RelationGetRelid(relation);
        return true;
    }

    return false;  // Tuple not found or not visible
}
```