# heap_hot_search_buffer

## Location
[src/backend/access/heap/heapam.c:1675-1826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1675-L1826)

## Overview
This function searches through a HOT (Heap-Only Tuples) chain to find the first tuple that satisfies a given snapshot's visibility requirements, optionally tracking whether all chain members are globally dead.

## Definition

```c
bool
heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
					   Snapshot snapshot, HeapTuple heapTuple,
					   bool *all_dead, bool first_call)
```
## Detailed Description
heap_hot_search_buffer traverses a HOT chain starting from a given TID to find a tuple visible to the specified snapshot. HOT chains are created when updates don't change indexed columns, allowing multiple tuple versions to share the same index entry. The function follows the chain by examining each tuple's t_ctid pointer, validating chain integrity through xmin/xmax relationships, and testing visibility.

The function handles both initial calls and continuation calls (when first_call is false), ensuring it doesn't return the same tuple repeatedly. It optionally tracks global deadness of all chain members for vacuum planning purposes and performs proper serializable isolation checks.

## Parameters / Member Variables
- : Input/output TID pointer; updated to point to the visible tuple if found
- : The heap relation containing the HOT chain
- : Pinned and locked buffer containing the page with the HOT chain
- : Snapshot for visibility testing
- : Caller-provided buffer filled with tuple data when a match is found
- : Optional output flag indicating if all chain members are globally dead
- : If true, this is the initial search; if false, skip the first tuple found

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md): Get page from buffer
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)/ItemPointerGetOffsetNumber: Extract TID components
  - [PageGetItemId](../P/PageGetItemId.md)/PageGetItem: Access page-level tuple data
  - ItemIdIsRedirected/ItemIdGetRedirect: Handle redirected item pointers
  - HeapTupleIsHeapOnly: Check if tuple is heap-only
  - [HeapTupleSatisfiesVisibility](../H/HeapTupleSatisfiesVisibility.md): Test tuple visibility against snapshot
  - [PredicateLockTID](../P/PredicateLockTID.md): Acquire predicate locks for serializable isolation
  - [GlobalVisTestFor](../G/GlobalVisTestFor.md)/HeapTupleIsSurelyDead: Test global tuple deadness
  - HeapTupleIsHotUpdated: Check if tuple continues the HOT chain
- Called from (representative examples):
  - [heapam_index_fetch_tuple](heapam_index_fetch_tuple.md): Index access method tuple fetching
  - [heapam_scan_bitmap_next_block](heapam_scan_bitmap_next_block.md): Bitmap scan implementation
  - [heap_index_delete_tuples](heap_index_delete_tuples.md): Index tuple deletion

## Notes and Other Information
- Requires caller to already have the buffer pinned and locked (unlike heap_fetch)
- Maintains HOT chain integrity by validating xmin/xmax transaction relationships
- Handles redirected item pointers that can appear at chain start
- The first_call/skip mechanism prevents returning the same tuple on continuation calls
- Global deadness tracking helps vacuum identify cleanup opportunities
- Performs serializable conflict detection and predicate locking for proper isolation
- Chain traversal stops at non-HOT-updated tuples or broken transaction relationships

## Simplified Source

```c
bool
heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
                       Snapshot snapshot, HeapTuple heapTuple,
                       bool *all_dead, bool first_call)
{
    Page page = BufferGetPage(buffer);
    TransactionId prev_xmax = InvalidTransactionId;
    BlockNumber blkno;
    OffsetNumber offnum;
    bool at_chain_start;
    bool valid;
    bool skip;
    GlobalVisState *vistest = NULL;

    // Initialize state based on whether this is first call
    if (all_dead)
        *all_dead = first_call;

    blkno = ItemPointerGetBlockNumber(tid);
    offnum = ItemPointerGetOffsetNumber(tid);
    at_chain_start = first_call;
    skip = !first_call;  // Skip first tuple on continuation calls

    // Traverse the HOT chain
    for (;;) {
        ItemId lp;

        // Validate offset number
        if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
            break;

        lp = PageGetItemId(page, offnum);

        // Handle non-normal items (deleted, redirected)
        if (!ItemIdIsNormal(lp)) {
            // Follow redirect only at chain start
            if (ItemIdIsRedirected(lp) && at_chain_start) {
                offnum = ItemIdGetRedirect(lp);
                at_chain_start = false;
                continue;
            }
            break;  // End of chain
        }

        // Fill in tuple information for current chain member
        heapTuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
        heapTuple->t_len = ItemIdGetLength(lp);
        heapTuple->t_tableOid = RelationGetRelid(relation);
        ItemPointerSet(&heapTuple->t_self, blkno, offnum);

        // Validate chain integrity
        if (at_chain_start && HeapTupleIsHeapOnly(heapTuple))
            break;  // Invalid: HEAP_ONLY at start

        if (TransactionId IsValid(prev_xmax) &&
            !TransactionIdEquals(prev_xmax, HeapTupleHeaderGetXmin(heapTuple->t_data)))
            break;  // Broken chain: xmin/xmax mismatch

        // Check visibility if not skipping this tuple
        if (!skip) {
            valid = HeapTupleSatisfiesVisibility(heapTuple, snapshot, buffer);
            HeapCheckForSerializableConflictOut(valid, relation, heapTuple,
                                               buffer, snapshot);

            if (valid) {
                // Found visible tuple - return it
                ItemPointerSetOffsetNumber(tid, offnum);
                PredicateLockTID(relation, &heapTuple->t_self, snapshot,
                               HeapTupleHeaderGetXmin(heapTuple->t_data));
                if (all_dead)
                    *all_dead = false;
                return true;
            }
        }
        skip = false;  // Don't skip subsequent tuples

        // Check if this tuple is globally dead (for vacuum planning)
        if (all_dead && *all_dead) {
            if (!vistest)
                vistest = GlobalVisTestFor(relation);
            if (!HeapTupleIsSurelyDead(heapTuple, vistest))
                *all_dead = false;
        }

        // Continue to next tuple in HOT chain if it exists
        if (HeapTupleIsHotUpdated(heapTuple)) {
            offnum = ItemPointerGetOffsetNumber(&heapTuple->t_data->t_ctid);
            at_chain_start = false;
            prev_xmax = HeapTupleHeaderGetUpdateXid(heapTuple->t_data);
        } else {
            break;  // End of chain
        }
    }

    return false;  // No visible tuple found
}
```