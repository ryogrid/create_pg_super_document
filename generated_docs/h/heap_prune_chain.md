# heap_prune_chain

## Location
[src/backend/access/heap/pruneheap.c:999-1200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L999-L1200)

## Overview
heap_prune_chain processes a HOT (Heap-Only Tuple) chain by determining the fate of each tuple in the chain and planning the appropriate pruning actions based on tuple visibility status.

## Definition
static void heap_prune_chain(Page page, BlockNumber blockno, OffsetNumber maxoff, OffsetNumber rootoffnum, PruneState *prstate)

## Detailed Description
This function implements the core logic for processing HOT chains during heap pruning operations. It traverses the entire chain starting from a root line pointer and determines the appropriate action for each tuple based on its visibility status. The function operates in several phases:

**Chain Traversal**: Follows the chain from root to end, validating each link by checking XMIN against the previous tuple's XMAX and ensuring HOT update relationships are maintained.

**Visibility-Based Processing**: For each tuple in the chain, uses cached HTSV results to determine if tuples are DEAD, RECENTLY_DEAD, or still live. DEAD tuples are candidates for removal, and RECENTLY_DEAD tuples preceding DEAD tuples are also considered removable.

**Pruning Strategy**: Implements three main strategies:
1. **No Dead Tuples**: Leave the entire chain unchanged
2. **Entire Chain Dead**: Mark root as LP_DEAD and remove all other tuples
3. **Partial Chain Dead**: Redirect root to first live tuple and remove dead predecessors

**Planning Phase**: Records planned changes in prstate arrays (redirected, nowdead, nowunused) rather than modifying the page directly. This allows the changes to be applied atomically later in a critical section.

The function ensures that no DEAD tuples with storage remain after pruning, as VACUUM cannot handle such cases.

## Parameters / Member Variables
- : The heap page containing the HOT chain
- : Block number of the page (for validation)
- : Maximum offset number on the page
- : Starting offset number of the HOT chain root
- : Pruning state containing visibility cache and change tracking arrays

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItemId](../P/PageGetItemId.md), PageGetItem
  - ItemId manipulation functions (ItemIdIsRedirected, ItemIdIsNormal, etc.)
  - HeapTupleHeader functions (HeapTupleHeaderGetXmin, HeapTupleHeaderIsHotUpdated, etc.)
  - [htsv_get_valid_status](htsv_get_valid_status.md) (for accessing cached visibility)
  - [HeapTupleHeaderAdvanceConflictHorizon](../H/HeapTupleHeaderAdvanceConflictHorizon.md)
  - heap_prune_record_* functions (for recording planned changes)
  - ItemPointer functions for following chain links
- Called from (representative examples):
  - [heap_page_prune_and_freeze](heap_page_prune_and_freeze.md)

## Notes and Other Information
- Static function internal to pruneheap.c
- Handles complex cases like broken redirect chains and partition movement validation
- Maintains conflict horizons for hot standby safety during tuple removal
- Uses cached HTSV results to avoid recomputing visibility
- Implements the "RECENTLY_DEAD preceding DEAD is also DEAD" optimization
- Critical for HOT optimization correctness - ensures chain integrity is maintained
- Defensive programming includes extensive assertions and error checking
- Part of the two-phase pruning approach (plan then execute)

## Simplified Source

```c
static void heap_prune_chain(Page page, BlockNumber blockno, OffsetNumber maxoff,
                             OffsetNumber rootoffnum, PruneState *prstate)
{
    TransactionId priorXmax = InvalidTransactionId;
    OffsetNumber offnum = rootoffnum;
    OffsetNumber chainitems[MaxHeapTuplesPerPage];
    int ndeadchain = 0, nchain = 0;

    // Traverse the HOT chain starting from root
    for (;;) {
        HeapTupleHeader htup;
        ItemId lp;

        // Basic validation and boundary checks
        if (offnum < FirstOffsetNumber || offnum > maxoff ||
            prstate->processed[offnum])
            break;

        lp = PageGetItemId(page, offnum);

        // Handle redirected line pointers (jump to actual tuple)
        if (ItemIdIsRedirected(lp)) {
            if (nchain > 0) break; // Not at chain start
            chainitems[nchain++] = offnum;
            offnum = ItemIdGetRedirect(lp);
            continue;
        }

        htup = (HeapTupleHeader) PageGetItem(page, lp);

        // Validate chain continuity (XMIN must match prior XMAX)
        if (TransactionIdIsValid(priorXmax) &&
            !TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax))
            break;

        chainitems[nchain++] = offnum;

        // Process based on tuple visibility status
        switch (htsv_get_valid_status(prstate->htsv[offnum])) {
            case HEAPTUPLE_DEAD:
                ndeadchain = nchain; // Mark position of last dead tuple
                HeapTupleHeaderAdvanceConflictHorizon(htup, &prstate->latest_xid_removed);
                break;

            case HEAPTUPLE_RECENTLY_DEAD:
                // Continue scanning for potential DEAD tuples
                break;

            case HEAPTUPLE_LIVE:
            case HEAPTUPLE_INSERT_IN_PROGRESS:
            case HEAPTUPLE_DELETE_IN_PROGRESS:
                goto process_chain; // Found live tuple, finish processing
        }

        // Follow chain to next tuple if HOT-updated
        if (!HeapTupleHeaderIsHotUpdated(htup))
            goto process_chain;

        offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
        priorXmax = HeapTupleHeaderGetUpdateXid(htup);
    }

process_chain:
    // Apply pruning strategy based on what we found
    if (ndeadchain == 0) {
        // No dead tuples - leave chain unchanged
        for (int i = 0; i < nchain; i++)
            heap_prune_record_unchanged_lp_normal(page, prstate, chainitems[i]);
    }
    else if (ndeadchain == nchain) {
        // Entire chain is dead - mark root dead, remove others
        heap_prune_record_dead_or_unused(prstate, rootoffnum, true);
        for (int i = 1; i < nchain; i++)
            heap_prune_record_unused(prstate, chainitems[i], true);
    }
    else {
        // Partial chain dead - redirect root to first live tuple
        heap_prune_record_redirect(prstate, rootoffnum, chainitems[ndeadchain], true);
        for (int i = 1; i < ndeadchain; i++)
            heap_prune_record_unused(prstate, chainitems[i], true);
        // Mark remaining tuples as unchanged
        for (int i = ndeadchain; i < nchain; i++)
            heap_prune_record_unchanged_lp_normal(page, prstate, chainitems[i]);
    }
}
```