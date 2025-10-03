# BitmapHeapNext

## Location
[src/backend/executor/nodeBitmapHeapscan.c:69-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapHeapscan.c#L69-L345)

## Overview
Retrieves the next tuple from a BitmapHeapScan node by iterating through bitmap results and fetching tuples from the underlying table relation.

## Definition
```c
static TupleTableSlot *
BitmapHeapNext(BitmapHeapScanState *node)
```

## Detailed Description
BitmapHeapNext is the core tuple-fetching function for bitmap heap scans in PostgreSQL. It implements an efficient scan strategy that uses a bitmap of tuple identifiers (TIDs) obtained from index scans to directly access relevant heap pages. The function handles both regular sequential bitmap iteration and parallel bitmap scans, with sophisticated prefetching mechanisms to optimize I/O performance.

The function operates in two main phases:
1. **Initialization phase**: Sets up bitmap iterators, table scan descriptors, and prefetching infrastructure when called for the first time
2. **Iteration phase**: Continuously fetches pages based on bitmap results, retrieves tuples from those pages, and applies recheck conditions for lossy bitmap entries

For prefetching optimization, the function maintains two separate iterators - one for actual scanning and another that runs ahead for prefetching pages. The prefetch distance starts small and gradually increases to avoid unnecessary I/O in queries that terminate early due to LIMIT clauses.

## Parameters / Member Variables
- `node`: BitmapHeapScanState containing scan state information including:
  - `tbm`: The TIDBitmap containing page/tuple location information
  - `tbmiterator`/`shared_tbmiterator`: Iterator(s) for traversing bitmap results
  - `tbmres`: Current bitmap iteration result
  - `prefetch_*`: Prefetching control variables
  - `initialized`: Flag indicating whether scan setup is complete

## Dependencies
- Functions called/Symbols referenced:
  - [MultiExecProcNode](../M/MultiExecProcNode.md): Execute child index scan to obtain bitmap
  - [tbm_begin_iterate](../t/tbm_begin_iterate.md)/`tbm_prepare_shared_iterate`: Initialize bitmap iterators
  - [tbm_iterate](../t/tbm_iterate.md)/`tbm_shared_iterate`: Get next bitmap page result
  - [BitmapAdjustPrefetchIterator](BitmapAdjustPrefetchIterator.md): Adjust prefetch iterator position
  - [BitmapAdjustPrefetchTarget](BitmapAdjustPrefetchTarget.md): Adjust prefetch distance target
  - [BitmapPrefetch](BitmapPrefetch.md): Issue prefetch requests for upcoming pages
  - [table_beginscan_bm](../t/table_beginscan_bm.md): Initialize bitmap table scan
  - `[table_scan_bitmap_next_block](../t/table_scan_bitmap_next_block.md)`: Position scanner at bitmap result block
  - `[table_scan_bitmap_next_tuple](../t/table_scan_bitmap_next_tuple.md)`: Fetch next tuple from current block
  - `[ExecQualAndReset](../E/ExecQualAndReset.md)`: Apply recheck conditions for lossy bitmap entries
  - [BitmapDoneInitializingSharedState](BitmapDoneInitializingSharedState.md): Signal completion of shared state setup
- Called from (representative examples):
  - [ExecBitmapHeapScan](../E/ExecBitmapHeapScan.md): Main execution function for BitmapHeapScan nodes

## Notes and Other Information
- The function supports both exact and lossy bitmap scans, with recheck logic for lossy entries
- Prefetching is conditionally compiled with USE_PREFETCH and uses adaptive prefetch distance control
- For parallel execution, the function coordinates shared state initialization among worker processes
- The scan can handle cases where no tuple data is needed (projection-only queries) by using table_beginscan_bm appropriately
- Error handling ensures that unexpected results from child index scans are detected and reported

## Simplified Source

```c
static TupleTableSlot *
BitmapHeapNext(BitmapHeapScanState *node)
{
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    TableScanDesc scan = node->ss.ss_currentScanDesc;
    TIDBitmap *tbm = node->tbm;
    TBMIterateResult *tbmres = node->tbmres;

    // One-time initialization: get bitmap from index scan and setup iterators
    if (!node->initialized) {
        // Get bitmap from child index scan node
        if (!node->pstate) {
            tbm = (TIDBitmap *) MultiExecProcNode(outerPlanState(node));
            if (!tbm || !IsA(tbm, TIDBitmap))
                elog(ERROR, "unrecognized result from subplan");

            node->tbm = tbm;
            node->tbmiterator = tbm_begin_iterate(tbm);
            // Setup prefetch iterator if enabled
            if (node->prefetch_maximum > 0) {
                node->prefetch_iterator = tbm_begin_iterate(tbm);
                node->prefetch_pages = 0;
                node->prefetch_target = -1;
            }
        } else {
            // Handle parallel bitmap scan initialization
            if (BitmapShouldInitializeSharedState(node->pstate)) {
                tbm = (TIDBitmap *) MultiExecProcNode(outerPlanState(node));
                node->tbm = tbm;
                node->pstate->tbmiterator = tbm_prepare_shared_iterate(tbm);
                BitmapDoneInitializingSharedState(node->pstate);
            }
            node->shared_tbmiterator = tbm_attach_shared_iterate(node->ss.ps.state->es_query_dsa,
                                                                 node->pstate->tbmiterator);
        }

        // Create table scan descriptor if needed
        if (!scan) {
            scan = table_beginscan_bm(node->ss.ss_currentRelation,
                                      node->ss.ps.state->es_snapshot,
                                      0, NULL, true);
            node->ss.ss_currentScanDesc = scan;
        }
        node->initialized = true;
    }

    // Main iteration loop: get pages from bitmap and fetch tuples
    for (;;) {
        CHECK_FOR_INTERRUPTS();

        // Get next page of results if needed
        if (tbmres == NULL) {
            if (!node->pstate)
                tbmres = tbm_iterate(node->tbmiterator);
            else
                tbmres = tbm_shared_iterate(node->shared_tbmiterator);

            if (tbmres == NULL)
                break;  // No more pages in bitmap

            node->tbmres = tbmres;
            BitmapAdjustPrefetchIterator(node, tbmres->blockno);

            // Position scan at this block
            bool valid_block = table_scan_bitmap_next_block(scan, tbmres);
            if (!valid_block)
                continue;  // Skip invalid blocks

            // Update page counters and adjust prefetch
            if (tbmres->ntuples >= 0)
                node->exact_pages++;
            else
                node->lossy_pages++;
            BitmapAdjustPrefetchTarget(node);
        }

        // Issue prefetch requests for upcoming pages
        BitmapPrefetch(node, scan);

        // Fetch next tuple from current page
        if (!table_scan_bitmap_next_tuple(scan, tbmres, slot)) {
            // No more tuples on this page, get next page
            node->tbmres = tbmres = NULL;
            continue;
        }

        // For lossy bitmap entries, recheck qual conditions
        if (tbmres->recheck) {
            econtext->ecxt_scantuple = slot;
            if (!ExecQualAndReset(node->bitmapqualorig, econtext)) {
                // Tuple fails recheck, try next one
                InstrCountFiltered2(node, 1);
                ExecClearTuple(slot);
                continue;
            }
        }

        // Return valid tuple
        return slot;
    }

    // End of scan reached
    return ExecClearTuple(slot);
}
```