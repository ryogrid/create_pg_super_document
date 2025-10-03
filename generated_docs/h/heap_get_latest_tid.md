# heap_get_latest_tid

## Location
[src/backend/access/heap/heapam.c:1827-1948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1827-L1948)

## Overview
This function follows a chain of tuple updates through t_ctid links to find the latest version of a row that is visible according to the scan's snapshot.

## Definition

```c
void
heap_get_latest_tid(TableScanDesc sscan,
					ItemPointer tid)
```
## Detailed Description
heap_get_latest_tid traverses the tuple update chain starting from a given TID by following t_ctid pointers until it reaches the end of the chain or encounters an invalid link. For each tuple in the chain, it validates the tuple's existence and checks its visibility against the provided snapshot. The function updates the input TID parameter to point to the latest visible version of the tuple.

The function performs integrity checks during traversal, including validation of xmin/xmax transaction relationships to ensure the chain hasn't been broken by concurrent operations. It stops traversal when encountering invalid tuples, broken transaction chains, or tuples that indicate the end of the update chain (through xmax invalidation, lock-only updates, or partition movement).

## Parameters / Member Variables
- `sscan`: Table scan descriptor containing relation and snapshot information
- `tid`: Input/output TID parameter; initially points to the starting tuple, updated to point to the latest visible version
## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffer](../R/ReadBuffer.md): Read and pin pages containing tuple versions
  - [LockBuffer](../L/LockBuffer.md)/UnlockReleaseBuffer: Manage buffer locks during access
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)/ItemPointerGetOffsetNumber: Extract TID components
  - [PageGetItemId](../P/PageGetItemId.md)/PageGetItem: Access page-level tuple data
  - [HeapTupleSatisfiesVisibility](../H/HeapTupleSatisfiesVisibility.md): Test tuple visibility against snapshot
  - [HeapCheckForSerializableConflictOut](../H/HeapCheckForSerializableConflictOut.md): Check for serializable conflicts
  - HeapTupleHeaderGetXmin/HeapTupleHeaderGetUpdateXid: Extract transaction IDs
  - [HeapTupleHeaderIsOnlyLocked](../H/HeapTupleHeaderIsOnlyLocked.md): Check if tuple is only locked
  - HeapTupleHeaderIndicatesMovedPartitions: Check for partition movement
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md): Used in tuple sampling operations
  - HeapScanIsValid: Part of heap scan validation

## Notes and Other Information
- The function modifies the input TID in-place to return the result
- Uses SnapshotDirty to get the very latest (possibly uncommitted) version
- Validates transaction chain integrity by checking xmin/xmax relationships
- Handles various tuple states: invalid xmax, lock-only updates, partition movement
- Does not optimize for single-version scenarios - always traverses the complete chain
- Performs serializable conflict detection for proper isolation levels
- Stops at self-referencing t_ctid pointers (indicates end of update chain)

## Simplified Source

```c
void
heap_get_latest_tid(TableScanDesc sscan, ItemPointer tid)
{
    Relation relation = sscan->rs_rd;
    Snapshot snapshot = sscan->rs_snapshot;
    ItemPointerData ctid;
    TransactionId priorXmax;

    // Start traversing from the input TID
    ctid = *tid;
    priorXmax = InvalidTransactionId;  // No prior transaction to check initially

    // Follow the t_ctid chain to find the latest visible version
    for (;;) {
        Buffer buffer;
        Page page;
        OffsetNumber offnum;
        ItemId lp;
        HeapTupleData tp;
        bool valid;

        // Read and lock the page containing the current tuple
        buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(&ctid));
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);

        // Validate the offset number
        offnum = ItemPointerGetOffsetNumber(&ctid);
        if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page)) {
            UnlockReleaseBuffer(buffer);
            break;  // Invalid offset, stop here
        }

        // Check if the item is valid (not deleted)
        lp = PageGetItemId(page, offnum);
        if (!ItemIdIsNormal(lp)) {
            UnlockReleaseBuffer(buffer);
            break;  // Deleted item, stop here
        }

        // Build tuple structure for this version
        tp.t_self = ctid;
        tp.t_data = (HeapTupleHeader) PageGetItem(page, lp);
        tp.t_len = ItemIdGetLength(lp);
        tp.t_tableOid = RelationGetRelid(relation);

        // Verify transaction chain integrity
        if (TransactionIdIsValid(priorXmax) &&
            !TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(tp.t_data))) {
            UnlockReleaseBuffer(buffer);
            break;  // Broken chain, stop here
        }

        // Check if this version is visible to our snapshot
        valid = HeapTupleSatisfiesVisibility(&tp, snapshot, buffer);
        HeapCheckForSerializableConflictOut(valid, relation, &tp, buffer, snapshot);

        if (valid) {
            *tid = ctid;  // Update result to this visible version
        }

        // Check if we've reached the end of the update chain
        if ((tp.t_data->t_infomask & HEAP_XMAX_INVALID) ||
            HeapTupleHeaderIsOnlyLocked(tp.t_data) ||
            HeapTupleHeaderIndicatesMovedPartitions(tp.t_data) ||
            ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid)) {
            UnlockReleaseBuffer(buffer);
            break;  // End of chain
        }

        // Move to the next tuple in the chain
        ctid = tp.t_data->t_ctid;
        priorXmax = HeapTupleHeaderGetUpdateXid(tp.t_data);
        UnlockReleaseBuffer(buffer);
    }
}
```