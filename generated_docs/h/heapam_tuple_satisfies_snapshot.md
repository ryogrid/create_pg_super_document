# heapam_tuple_satisfies_snapshot

## Location
[src/backend/access/heap/heapam_handler.c:214-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L214-L241)

## Overview
This function determines whether a tuple stored in a table slot satisfies the visibility requirements of a given snapshot, providing thread-safe access to the tuple's visibility information through proper buffer locking.

## Definition
```c
static bool
heapam_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
                                Snapshot snapshot)
```

## Detailed Description
heapam_tuple_satisfies_snapshot is a static callback function used by the heap access method to check tuple visibility according to MVCC (Multi-Version Concurrency Control) rules. The function extracts the tuple from a BufferHeapTupleTableSlot and calls the core HeapTupleSatisfiesVisibility function to determine if the tuple should be visible to the transaction represented by the given snapshot.

The function implements proper concurrency control by acquiring a shared lock on the buffer containing the tuple before performing the visibility check, ensuring that the tuple's header information remains stable during the visibility determination. This is crucial for maintaining data consistency in a multi-user environment.

## Parameters / Member Variables
- `rel`: The heap relation containing the tuple (used for context but not directly accessed in this function)
- `slot`: TupleTableSlot containing the tuple to check (must be a BufferHeapTupleTableSlot with a valid buffer)
- `snapshot`: Snapshot representing the transaction's view of the database for visibility checking

## Dependencies
- Functions called/Symbols referenced:
  - [BufferHeapTupleTableSlot](../B/BufferHeapTupleTableSlot.md) (type cast)
  - TTS_IS_BUFFERTUPLE (assertion check)
  - [BufferIsValid](../B/BufferIsValid.md) (assertion check)
  - [LockBuffer](../L/LockBuffer.md) (buffer locking with BUFFER_LOCK_SHARE and BUFFER_LOCK_UNLOCK)
  - [HeapTupleSatisfiesVisibility](../H/HeapTupleSatisfiesVisibility.md) (core visibility checking)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)

## Notes and Other Information
- This is a static function serving as a callback in the table access method interface
- Requires that the slot parameter is of type BufferHeapTupleTableSlot and asserts this condition
- Assumes the caller holds a buffer pin but not a lock, and temporarily acquires a shared lock for the visibility check
- The function follows proper locking protocol: acquire shared lock, perform visibility check, release lock
- Returns true if the tuple is visible according to the snapshot, false otherwise
- Part of the heap access method's MVCC infrastructure for transaction isolation
- The buffer locking ensures that concurrent modifications don't interfere with visibility determination