# heap_fetch

## Location
[src/backend/access/heap/heapam.c:1555-1674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1555-L1674)

## Overview
This function retrieves a specific tuple from a heap relation using its TID (tuple identifier), performs visibility checks against a snapshot, and manages buffer pinning for the caller.

## Definition

```c
bool
heap_fetch(Relation relation,
		   Snapshot snapshot,
		   HeapTuple tuple,
		   Buffer *userbuf,
		   bool keep_buf)
```
## Detailed Description
heap_fetch is a low-level tuple retrieval function that fetches a tuple directly by its TID without following HOT (Heap-Only Tuples) chains. The function reads the appropriate page, validates the tuple's existence and visibility according to the provided snapshot, and returns the tuple data along with a pinned buffer reference.

The function performs several validation steps: it checks for valid block and offset numbers, ensures the item pointer references a normal (not deleted) tuple, fills in the tuple structure, and performs visibility testing. Depending on the keep_buf parameter and visibility results, it either releases the buffer or leaves it pinned for the caller to manage.

## Parameters / Member Variables
- `relation`: The heap relation from which to fetch the tuple
- `snapshot`: Snapshot for visibility checking and consistency
- `tuple`: HeapTuple structure with t_self set to the target TID; filled in with tuple data on success
- `*userbuf`: Output parameter set to the pinned buffer containing the tuple (or InvalidBuffer on failure)
- `keep_buf`: If true, keeps buffer pinned even when tuple fails visibility check
## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffer](../R/ReadBuffer.md): Read and pin the page containing the tuple
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)/ItemPointerGetOffsetNumber: Extract TID components
  - [LockBuffer](../L/LockBuffer.md): Acquire/release buffer locks for safe access
  - [PageGetItemId](../P/PageGetItemId.md)/PageGetItem: Access page-level tuple data
  - [HeapTupleSatisfiesVisibility](../H/HeapTupleSatisfiesVisibility.md): Check tuple visibility against snapshot
  - [PredicateLockTID](../P/PredicateLockTID.md): Acquire predicate locks for serializable isolation
  - [HeapCheckForSerializableConflictOut](../H/HeapCheckForSerializableConflictOut.md): Check for serializable conflicts
- Called from (representative examples):
  - [heapam_fetch_row_version](heapam_fetch_row_version.md): Table AM interface implementation
  - [heapam_tuple_lock](heapam_tuple_lock.md): Tuple locking implementation
  - [heap_lock_updated_tuple_rec](heap_lock_updated_tuple_rec.md): Recursive tuple locking

## Notes and Other Information
- Does not follow HOT chains - fetches only the exact TID specified
- Error handling is asymmetric: invalid blocks cause ereport(), invalid items return false
- When keep_buf is true, caller must always unpin the buffer regardless of return value
- Performs predicate locking and serializable conflict detection for proper isolation
- The function assumes tuple->t_self contains the target TID on entry
- Buffer management is critical: successful calls require caller to release the buffer

## Simplified Source

```c
bool
heap_fetch(Relation relation,
           Snapshot snapshot,
           HeapTuple tuple,
           Buffer *userbuf,
           bool keep_buf)
{
    ItemPointer tid = &(tuple->t_self);
    Buffer buffer;
    Page page;
    OffsetNumber offnum;
    ItemId lp;
    bool valid;

    // Read and pin the page containing the tuple
    buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));

    // Lock buffer to examine tuple safely
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);

    // Validate offset number is within page bounds
    offnum = ItemPointerGetOffsetNumber(tid);
    if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page)) {
        // Invalid offset - cleanup and return false
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
        *userbuf = InvalidBuffer;
        tuple->t_data = NULL;
        return false;
    }

    // Get the item pointer for this offset
    lp = PageGetItemId(page, offnum);

    // Check if tuple is deleted/invalid
    if (!ItemIdIsNormal(lp)) {
        // Deleted tuple - cleanup and return false
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
        *userbuf = InvalidBuffer;
        tuple->t_data = NULL;
        return false;
    }

    // Fill in tuple structure with data from page
    tuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
    tuple->t_len = ItemIdGetLength(lp);
    tuple->t_tableOid = RelationGetRelid(relation);

    // Check if tuple is visible according to snapshot
    valid = HeapTupleSatisfiesVisibility(tuple, snapshot, buffer);

    // Handle predicate locking and serializable conflict detection
    if (valid) {
        PredicateLockTID(relation, &(tuple->t_self), snapshot,
                        HeapTupleHeaderGetXmin(tuple->t_data));
    }
    HeapCheckForSerializableConflictOut(valid, relation, tuple, buffer, snapshot);

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    // Return results based on visibility and keep_buf setting
    if (valid) {
        *userbuf = buffer;  // Caller must unpin
        return true;
    }

    // Tuple failed visibility check
    if (keep_buf) {
        *userbuf = buffer;  // Keep buffer even though tuple not visible
    } else {
        ReleaseBuffer(buffer);
        *userbuf = InvalidBuffer;
        tuple->t_data = NULL;
    }

    return false;
}
```