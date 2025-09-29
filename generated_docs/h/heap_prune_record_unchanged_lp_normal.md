# heap_prune_record_unchanged_lp_normal

## Location
[src/backend/access/heap/pruneheap.c:1330-1507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L1330-L1507)

## Overview
Records a normal line pointer that remains unchanged during heap page pruning, handling visibility checks, tuple counting, and freeze planning for live tuples.

## Definition
```c
static void heap_prune_record_unchanged_lp_normal(Page page, PruneState *prstate, OffsetNumber offnum)
```

## Detailed Description
This function is a critical component of PostgreSQL's heap page pruning mechanism that handles normal (non-dead, non-unused) line pointers that don't need to be removed during pruning. Unlike the simpler unchanged functions for dead/unused line pointers, this function performs comprehensive analysis of live tuples including:

1. **Tuple State Analysis**: Examines the tuple's visibility state (LIVE, RECENTLY_DEAD, INSERT_IN_PROGRESS, DELETE_IN_PROGRESS) and updates appropriate counters
2. **Visibility Map Management**: Determines if the page can be marked as all-visible by checking transaction visibility and commit status
3. **Freeze Planning**: When freezing is enabled, prepares freeze plans for tuples to prevent transaction ID wraparound
4. **Statistics Maintenance**: Updates live tuple and recently dead tuple counts that are used by VACUUM and ANALYZE

The function must maintain consistency with ANALYZE's acquire_sample_rows() function to ensure that VACUUM and ANALYZE produce compatible tuple count statistics.

## Parameters / Member Variables
- `page`: The heap page containing the tuple being processed
- `prstate`: Pointer to the pruning state structure containing counters, visibility info, and freeze plans
- `offnum`: The offset number of the line pointer being recorded as unchanged

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItem](../P/PageGetItem.md), PageGetItemId (page item access)
  - HeapTupleHeaderXminCommitted, HeapTupleHeaderGetXmin, HeapTupleHeaderGetUpdateXid (tuple header access)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md), TransactionIdFollows, TransactionIdIsNormal (transaction ID comparison)
  - [heap_prune_record_prunable](heap_prune_record_prunable.md) (records prunable transaction IDs)
  - [heap_prepare_freeze_tuple](heap_prepare_freeze_tuple.md) (prepares tuple freezing)
- Called from (representative examples):
  - [heap_page_prune_and_freeze](heap_page_prune_and_freeze.md)
  - [heap_prune_chain](heap_prune_chain.md)

## Notes and Other Information
- This is the most complex of the heap_prune_record_unchanged_lp_* family of functions
- Handles different tuple visibility states with specific logic for each case
- Critical for maintaining accurate tuple statistics that VACUUM and ANALYZE depend on
- Integrates with PostgreSQL's visibility map and freeze mechanisms
- The function assumes VACUUM cannot run inside a transaction block, simplifying some visibility logic
- Contains extensive comments explaining the relationship with ANALYZE's tuple counting methodology

## Simplified Source

```c
static void heap_prune_record_unchanged_lp_normal(Page page, PruneState *prstate, OffsetNumber offnum)
{
    HeapTupleHeader htup;

    // Mark as processed and indicate page has tuples
    prstate->processed[offnum] = true;
    prstate->hastup = true;

    htup = (HeapTupleHeader) PageGetItem(page, PageGetItemId(page, offnum));

    // Handle tuple based on its visibility status
    switch (prstate->htsv[offnum]) {
        case HEAPTUPLE_LIVE:
            prstate->live_tuples++;

            // Check if tuple is visible to all transactions (for visibility map)
            if (prstate->all_visible && prstate->cutoffs) {
                TransactionId xmin = HeapTupleHeaderGetXmin(htup);

                if (!HeapTupleHeaderXminCommitted(htup) ||
                    !TransactionIdPrecedes(xmin, prstate->cutoffs->OldestXmin)) {
                    prstate->all_visible = false;
                } else {
                    // Track newest xmin for visibility cutoff
                    if (TransactionIdFollows(xmin, prstate->visibility_cutoff_xid))
                        prstate->visibility_cutoff_xid = xmin;
                }
            }
            break;

        case HEAPTUPLE_RECENTLY_DEAD:
            prstate->recently_dead_tuples++;
            prstate->all_visible = false;
            // Mark page as needing future pruning
            heap_prune_record_prunable(prstate, HeapTupleHeaderGetUpdateXid(htup));
            break;

        case HEAPTUPLE_INSERT_IN_PROGRESS:
            prstate->all_visible = false;
            break;

        case HEAPTUPLE_DELETE_IN_PROGRESS:
            prstate->live_tuples++;
            prstate->all_visible = false;
            heap_prune_record_prunable(prstate, HeapTupleHeaderGetUpdateXid(htup));
            break;
    }

    // Consider freezing the tuple if freeze mode is enabled
    if (prstate->freeze) {
        bool totally_frozen;

        if (heap_prepare_freeze_tuple(htup, prstate->cutoffs, &prstate->pagefrz,
                                     &prstate->frozen[prstate->nfrozen], &totally_frozen)) {
            prstate->frozen[prstate->nfrozen++].offset = offnum;
        }

        if (!totally_frozen)
            prstate->all_frozen = false;
    }
}
```