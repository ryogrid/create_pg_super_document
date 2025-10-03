# IndexOnlyNext

## Location
[src/backend/executor/nodeIndexonlyscan.c:61-267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexonlyscan.c#L61-L267)

## Overview
Retrieves the next tuple from an index-only scan operation, attempting to avoid heap access when possible by utilizing the visibility map to determine tuple visibility.

## Definition

```c
static TupleTableSlot *
IndexOnlyNext(IndexOnlyScanState *node)
```
## Detailed Description
The IndexOnlyNext function is the core tuple retrieval mechanism for index-only scans in PostgreSQL. It implements an optimization where data can be returned directly from the index without accessing the heap table, provided that all tuples on the relevant heap page are visible to all transactions (as indicated by the visibility map).

The function first initializes or reuses an index scan descriptor, then enters a loop to fetch TuDs from the index. For each TID, it checks the visibility map to determine if a heap access is necessary. If the visibility map indicates that all tuples on the page are visible, the function can return data directly from the index. Otherwise, it performs a heap fetch to verify tuple visibility.

The function also handles lossy index scans by rechecking index qualifiers when necessary, and maintains proper predicate locking for serializable isolation levels.

## Parameters / Member Variables
- `*node`: IndexOnlyScanState containing scan state information including scan descriptors, relation information, scan keys, and slot references
## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionCombine: Combines plan and execution scan directions
  - [index_beginscan](../i/index_beginscan.md): Initiates a new index scan operation
  - [index_rescan](../i/index_rescan.md): Restarts an index scan with new parameters  
  - [index_getnext_tid](../i/index_getnext_tid.md): Retrieves the next TID from the index scan
  - VM_ALL_VISIBLE: Checks if all tuples on a heap page are visible
  - [index_fetch_heap](../i/index_fetch_heap.md): Fetches the actual heap tuple for visibility checking
  - [StoreIndexTuple](../S/StoreIndexTuple.md): Stores index tuple data into the scan slot
  - [ExecQualAndReset](../E/ExecQualAndReset.md): Rechecks index qualifiers for lossy scans
  - [PredicateLockPage](../P/PredicateLockPage.md): Acquires predicate locks for serializable isolation
- Called from (representative examples):
  - [ExecIndexOnlyScan](../E/ExecIndexOnlyScan.md): Main execution function for index-only scan nodes

## Notes and Other Information
- The function implements a sophisticated visibility checking mechanism using the visibility map to avoid unnecessary heap accesses
- Includes detailed memory ordering considerations to handle concurrent inserts and deletes safely
- Only supports MVCC snapshots and will error on non-MVCC snapshot types
- Does not support rechecking ORDER BY distances for lossy scans
- Maintains buffer pins on heap pages across calls for potential reuse
- Returns an empty slot when the scan is exhausted

## Simplified Source

```c
static TupleTableSlot *IndexOnlyNext(IndexOnlyScanState *node)
{
    EState *estate = node->ss.ps.state;
    ScanDirection direction = ScanDirectionCombine(estate->es_direction,
                                                   ((IndexOnlyScan *) node->ss.ps.plan)->indexorderdir);
    IndexScanDesc scandesc = node->ioss_ScanDesc;
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    ItemPointer tid;

    // Initialize scan descriptor if needed
    if (scandesc == NULL)
    {
        scandesc = index_beginscan(node->ss.ss_currentRelation,
                                   node->ioss_RelationDesc,
                                   estate->es_snapshot,
                                   node->ioss_NumScanKeys,
                                   node->ioss_NumOrderByKeys);
        node->ioss_ScanDesc = scandesc;

        // Configure for index-only scan
        scandesc->xs_want_itup = true;
        node->ioss_VMBuffer = InvalidBuffer;

        // Pass scan keys if ready
        if (node->ioss_NumRuntimeKeys == 0 || node->ioss_RuntimeKeysReady)
            index_rescan(scandesc, node->ioss_ScanKeys, node->ioss_NumScanKeys,
                         node->ioss_OrderByKeys, node->ioss_NumOrderByKeys);
    }

    // Main scan loop
    while ((tid = index_getnext_tid(scandesc, direction)) != NULL)
    {
        bool tuple_from_heap = false;

        CHECK_FOR_INTERRUPTS();

        // Check visibility map to avoid heap access if possible
        if (!VM_ALL_VISIBLE(scandesc->heapRelation,
                            ItemPointerGetBlockNumber(tid),
                            &node->ioss_VMBuffer))
        {
            // Need to check heap for visibility
            InstrCountTuples2(node, 1);
            if (!index_fetch_heap(scandesc, node->ioss_TableSlot))
                continue; // Tuple not visible, try next

            ExecClearTuple(node->ioss_TableSlot);

            // Verify MVCC snapshot support
            if (scandesc->xs_heap_continue)
                elog(ERROR, "non-MVCC snapshots are not supported in index-only scans");

            tuple_from_heap = true;
        }

        // Store tuple data from index into slot
        if (scandesc->xs_hitup)
        {
            Assert(slot->tts_tupleDescriptor->natts == scandesc->xs_hitupdesc->natts);
            ExecForceStoreHeapTuple(scandesc->xs_hitup, slot, false);
        }
        else if (scandesc->xs_itup)
            StoreIndexTuple(node, slot, scandesc->xs_itup, scandesc->xs_itupdesc);
        else
            elog(ERROR, "no data returned for index-only scan");

        // Recheck index quals if scan was lossy
        if (scandesc->xs_recheck)
        {
            econtext->ecxt_scantuple = slot;
            if (!ExecQualAndReset(node->recheckqual, econtext))
            {
                InstrCountFiltered2(node, 1);
                continue; // Failed recheck, try next tuple
            }
        }

        // Error if ORDER BY distance rechecking is needed (not supported)
        if (scandesc->numberOfOrderBys > 0 && scandesc->xs_recheckorderby)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("lossy distance functions are not supported in index-only scans")));

        // Take predicate lock if we didn't access heap
        if (!tuple_from_heap)
            PredicateLockPage(scandesc->heapRelation,
                              ItemPointerGetBlockNumber(tid),
                              estate->es_snapshot);

        return slot;
    }

    // End of scan - return empty slot
    return ExecClearTuple(slot);
}
```