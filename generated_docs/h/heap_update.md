# heap_update

## Location
[src/backend/access/heap/heapam.c:3200-4181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L3200-L4181)

## Overview
heap_update is the core function responsible for replacing a tuple in a heap table, handling complex visibility rules, hot updates, toast management, and multi-version concurrency control to ensure ACID compliance.

## Definition

```c
TM_Result
heap_update(Relation relation, ItemPointer otid, HeapTuple newtup,
			CommandId cid, Snapshot crosscheck, bool wait,
			TM_FailureData *tmfd, LockTupleMode *lockmode,
			TU_UpdateIndexes *update_indexes)
```
## Detailed Description
heap_update performs the low-level replacement of a heap tuple with comprehensive transaction safety, concurrency control, and optimization strategies. This function is one of the most complex operations in PostgreSQL's heap access method, implementing sophisticated logic for:

**Hot Updates Optimization**: When possible, performs HOT (Heap-Only Tuple) updates that avoid index maintenance by placing the new tuple on the same page and not modifying indexed columns.

**Toast Management**: Handles out-of-line storage for large attributes, potentially compressing or moving data to separate TOAST tables.

**Concurrency Control**: Uses HeapTupleSatisfiesUpdate to check tuple visibility and manages complex multi-transaction scenarios, including waiting for conflicting operations and preserving necessary locks.

**Key Column Detection**: Analyzes which columns are being modified to determine appropriate locking levels - non-key updates can use weaker locks allowing more concurrency.

**Space Management**: Determines whether the updated tuple can fit on the same page or requires a new page, handling the complex buffer management and deadlock avoidance required for cross-page updates.

**Replica Identity**: Extracts and preserves replica identity information needed for logical replication.

The function operates through several phases:
1. **Preparation**: Validates parameters, determines column modifications, and acquires necessary bitmap sets
2. **Concurrency Handling**: Checks tuple visibility, handles concurrent modifications, and establishes appropriate locking
3. **Space Planning**: Determines if TOAST processing or new page allocation is needed
4. **Critical Section**: Updates tuple headers, manages visibility information, and logs changes
5. **Cleanup**: Handles resource cleanup and statistics updates

## Parameters / Member Variables
- `relation`: The heap relation containing the tuple to update
- `otid`: ItemPointer identifying the location of the tuple to be updated
- `newtup`: HeapTuple containing the new tuple data to replace the old tuple
- `cid`: Command identifier for the current command within the transaction
- `crosscheck`: Optional snapshot for additional visibility validation (used in RI checks)
- `wait`: Boolean indicating whether to wait for concurrent transactions or return immediately
- `*tmfd`: Output structure containing failure details when update cannot proceed
- `*lockmode`: Input/output parameter for the type of tuple lock required/acquired
- `*update_indexes`: Output parameter indicating which indexes need updating after the operation
## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleSatisfiesUpdate](../H/HeapTupleSatisfiesUpdate.md)
  - [HeapDetermineColumnsInfo](../H/HeapDetermineColumnsInfo.md)
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md)
  - [RelationGetIndexAttrBitmap](../R/RelationGetIndexAttrBitmap.md)
  - [ExtractReplicaIdentity](../E/ExtractReplicaIdentity.md)
  - [heap_toast_insert_or_update](heap_toast_insert_or_update.md)
  - [RelationGetBufferForTuple](../R/RelationGetBufferForTuple.md)
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md)
  - [log_heap_update](../l/log_heap_update.md)
  - [check_lock_if_inplace_updateable_rel](../c/check_lock_if_inplace_updateable_rel.md)
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md)
  - [heap_freetuple](heap_freetuple.md)
- Called from (representative examples):
  - [simple_heap_update](../s/simple_heap_update.md)
  - [heapam_tuple_update](heapam_tuple_update.md)

## Notes and Other Information
- The function prohibits execution during parallel operations to prevent combo CID allocation issues
- Implements sophisticated HOT update optimization to avoid index maintenance when possible
- Handles both same-page and cross-page updates with appropriate deadlock prevention
- Supports summarized updates for improved performance with certain index types
- Uses critical sections to ensure atomic updates that can be properly recovered from crashes
- The function can return various TM_Result codes indicating success, conflicts, or conditions requiring caller attention
- Manages complex tuple locking scenarios including multi-transaction preservation
- Optimizes locking by using weaker locks when key columns are not modified
- Performs extensive validation including assertion checking in debug builds
- Handles toast table relationships correctly, never recursively toasting toast table entries

## Simplified Source

```c
TM_Result heap_update(Relation relation, ItemPointer otid, HeapTuple newtup,
                     CommandId cid, Snapshot crosscheck, bool wait,
                     TM_FailureData *tmfd, LockTupleMode *lockmode,
                     TU_UpdateIndexes *update_indexes) {
    TransactionId xid = GetCurrentTransactionId();
    Buffer buffer, newbuf;
    Page page;
    HeapTupleData oldtup;
    bool use_hot_update = false;
    bool need_toast;

    // Get attribute bitmaps for HOT and key column analysis
    Bitmapset *hot_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_HOT_BLOCKING);
    Bitmapset *key_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_KEY);
    Bitmapset *modified_attrs;

    // Read the old tuple
    buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(otid));
    page = BufferGetPage(buffer);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    // Validate old tuple exists and is accessible
    ItemId lp = PageGetItemId(page, ItemPointerGetOffsetNumber(otid));
    if (!ItemIdIsNormal(lp)) {
        // Tuple was pruned/deleted
        UnlockReleaseBuffer(buffer);
        return TM_Deleted;
    }

    // Setup old tuple structure
    oldtup.t_data = (HeapTupleHeader) PageGetItem(page, lp);
    oldtup.t_len = ItemIdGetLength(lp);
    oldtup.t_self = *otid;

    // Determine which columns were modified
    modified_attrs = HeapDetermineColumnsInfo(relation, interesting_attrs,
                                             id_attrs, &oldtup, newtup, NULL);

    // Choose lock mode based on whether key columns changed
    if (!bms_overlap(modified_attrs, key_attrs)) {
        *lockmode = LockTupleNoKeyExclusive;
        // Can potentially do HOT update
    } else {
        *lockmode = LockTupleExclusive;
    }

    // Check if tuple can be updated (visibility and concurrency)
    TM_Result result = HeapTupleSatisfiesUpdate(&oldtup, cid, buffer);
    if (result == TM_BeingModified && wait) {
        // Handle concurrent modification - wait or check conflicts
        // (Complex concurrency logic simplified)
        goto l2; // Retry after waiting
    }

    if (result != TM_Ok) {
        // Update failed due to concurrent changes
        UnlockReleaseBuffer(buffer);
        return result;
    }

    // Determine if TOAST processing is needed
    need_toast = (HeapTupleHasExternal(&oldtup) ||
                  HeapTupleHasExternal(newtup) ||
                  newtup->t_len > TOAST_TUPLE_THRESHOLD);

    // Check if new tuple fits on same page
    Size pagefree = PageGetHeapFreeSpace(page);
    Size newtupsize = MAXALIGN(newtup->t_len);

    if (need_toast || newtupsize > pagefree) {
        // Need TOAST processing or new page
        if (need_toast) {
            HeapTuple heaptup = heap_toast_insert_or_update(relation, newtup, &oldtup, 0);
            newtupsize = MAXALIGN(heaptup->t_len);
        }

        if (newtupsize > pagefree) {
            // Get new page for tuple
            newbuf = RelationGetBufferForTuple(relation, newtup->t_len, buffer, 0, NULL, NULL, NULL, 0);
        } else {
            newbuf = buffer;
        }
    } else {
        // Fits on same page
        newbuf = buffer;
    }

    // Check for HOT update possibility
    if (newbuf == buffer && !bms_overlap(modified_attrs, hot_attrs)) {
        use_hot_update = true;
    }

    START_CRIT_SECTION();

    // Update tuple headers and visibility information
    if (use_hot_update) {
        HeapTupleSetHotUpdated(&oldtup);
        HeapTupleSetHeapOnly(newtup);
    }

    // Insert new tuple
    RelationPutHeapTuple(relation, newbuf, newtup, false);

    // Update old tuple's ctid to point to new tuple
    oldtup.t_data->t_ctid = newtup->t_self;

    // Clear visibility map bits and mark buffers dirty
    if (PageIsAllVisible(page)) {
        PageClearAllVisible(page);
        visibilitymap_clear(relation, BufferGetBlockNumber(buffer), NULL, VISIBILITYMAP_VALID_BITS);
    }

    MarkBufferDirty(buffer);
    if (newbuf != buffer) {
        MarkBufferDirty(newbuf);
    }

    // WAL logging
    if (RelationNeedsWAL(relation)) {
        XLogRecPtr recptr = log_heap_update(relation, buffer, newbuf, &oldtup, newtup,
                                          NULL, all_visible_cleared, all_visible_cleared_new);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    // Release locks and cleanup
    UnlockReleaseBuffer(buffer);
    if (newbuf != buffer) {
        UnlockReleaseBuffer(newbuf);
    }

    // Set index update requirement
    if (use_hot_update) {
        *update_indexes = summarized_update ? TU_Summarizing : TU_None;
    } else {
        *update_indexes = TU_All;
    }

    return TM_Ok;
}
```