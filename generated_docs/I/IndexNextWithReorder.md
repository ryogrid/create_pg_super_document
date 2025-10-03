# IndexNextWithReorder

## Location
[src/backend/executor/nodeIndexscan.c:168-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L168-L359)

## Overview
The IndexNextWithReorder function retrieves tuples from an index scan and maintains proper ordering when the index's ORDER BY values might be inaccurate, using a reorder queue to ensure correct tuple ordering.

## Definition
```c
static TupleTableSlot *IndexNextWithReorder(IndexScanState *node)
```

## Detailed Description
IndexNextWithReorder is an enhanced version of IndexNext that handles cases where the index access method returns tuples with potentially inaccurate ORDER BY values. This function implements a sophisticated reordering mechanism to maintain correct ordering:

1. **Direction Validation**: Only supports forward scan direction since reordering with backward scans is not supported
2. **Reorder Queue Management**: Maintains a priority queue (pairing heap) to store tuples that might be out of order
3. **Queue-First Processing**: Always checks the reorder queue first to return tuples in the correct order
4. **Index Tuple Fetching**: Retrieves tuples from the index using index_getnext_slot
5. **Lossy Index Handling**: Performs recheck qualification for lossy indexes using ExecQualAndReset
6. **ORDER BY Rechecking**: When xs_recheckorderby is set, recalculates ORDER BY expressions using EvalOrderByExpressions
7. **Order Validation**: Compares index-provided ORDER BY values with recalculated values using cmp_orderbyvals
8. **Smart Queuing**: Only queues tuples when necessary (when ORDER BY values were inexact or when smaller tuples exist in the queue)
9. **End-of-Scan Handling**: Drains remaining tuples from the queue after the index is exhausted

The function ensures that tuples are returned in the correct ORDER BY sequence even when the underlying index access method provides approximate ordering.

## Parameters / Member Variables
- `node`: IndexScanState structure containing scan state, reorder queue, ORDER BY information, and tuple slots

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsBackward
  - ScanDirectionIsForward  
  - [index_beginscan](../i/index_beginscan.md)
  - [index_rescan](../i/index_rescan.md)
  - pairingheap_is_empty
  - [pairingheap_first](../p/pairingheap_first.md)
  - [cmp_orderbyvals](../c/cmp_orderbyvals.md)
  - [reorderqueue_pop](../r/reorderqueue_pop.md)
  - [ExecForceStoreHeapTuple](../E/ExecForceStoreHeapTuple.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [index_getnext_slot](../i/index_getnext_slot.md)
  - [ExecQualAndReset](../E/ExecQualAndReset.md)
  - InstrCountFiltered2
  - ResetExprContext
  - [EvalOrderByExpressions](../E/EvalOrderByExpressions.md)
  - [reorderqueue_push](../r/reorderqueue_push.md)
  - CHECK_FOR_INTERRUPTS
- Called from (representative examples):
  - ReorderTuple (nodeIndexscan.c:59)
  - [ExecIndexScan](../E/ExecIndexScan.md) (nodeIndexscan.c:531)

## Notes and Other Information
- This is a static function used internally within the index scan executor
- Essential for index access methods that support ORDER BY but may return inexact ordering (e.g., GiST indexes)
- The reorder queue prevents incorrect ordering when the index returns tuples with approximate ORDER BY values
- Includes comprehensive error checking to detect when index returns tuples in wrong order
- Performance optimization: avoids unnecessary queuing when ORDER BY values are exact and no reordering is needed
- Only supports forward scans due to the complexity of maintaining ordering in reverse with inexact ORDER BY values
- The function properly handles interruption for long-running operations and maintains instrumentation counters

## Simplified Source

```c
static TupleTableSlot *IndexNextWithReorder(IndexScanState *node)
{
    EState *estate = node->ss.ps.state;
    IndexScanDesc scandesc = node->iss_ScanDesc;
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    ReorderTuple *topmost = NULL;
    bool was_exact;
    Datum *lastfetched_vals;
    bool *lastfetched_nulls;
    int cmp;

    // Only forward scans are supported with reordering
    Assert(!ScanDirectionIsBackward(((IndexScan *) node->ss.ps.plan)->indexorderdir));
    Assert(ScanDirectionIsForward(estate->es_direction));

    // Initialize scan descriptor if needed
    if (scandesc == NULL)
    {
        scandesc = index_beginscan(node->ss.ss_currentRelation,
                                   node->iss_RelationDesc,
                                   estate->es_snapshot,
                                   node->iss_NumScanKeys,
                                   node->iss_NumOrderByKeys);
        node->iss_ScanDesc = scandesc;

        if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
            index_rescan(scandesc, node->iss_ScanKeys, node->iss_NumScanKeys,
                         node->iss_OrderByKeys, node->iss_NumOrderByKeys);
    }

    for (;;)
    {
        CHECK_FOR_INTERRUPTS();

        // Check reorder queue first - return smallest tuple if ready
        if (!pairingheap_is_empty(node->iss_ReorderQueue))
        {
            topmost = (ReorderTuple *) pairingheap_first(node->iss_ReorderQueue);

            if (node->iss_ReachedEnd ||
                cmp_orderbyvals(topmost->orderbyvals, topmost->orderbynulls,
                                scandesc->xs_orderbyvals, scandesc->xs_orderbynulls,
                                node) <= 0)
            {
                HeapTuple tuple = reorderqueue_pop(node);
                ExecForceStoreHeapTuple(tuple, slot, true);
                return slot;
            }
        }
        else if (node->iss_ReachedEnd)
        {
            // Queue empty and scan done
            return ExecClearTuple(slot);
        }

        // Fetch next tuple from index
        if (!index_getnext_slot(scandesc, ForwardScanDirection, slot))
        {
            node->iss_ReachedEnd = true;
            continue; // Drain remaining queue tuples
        }

        // Recheck index quals for lossy indexes
        if (scandesc->xs_recheck)
        {
            econtext->ecxt_scantuple = slot;
            if (!ExecQualAndReset(node->indexqualorig, econtext))
            {
                InstrCountFiltered2(node, 1);
                CHECK_FOR_INTERRUPTS();
                continue;
            }
        }

        // Handle ORDER BY rechecking if needed
        if (scandesc->xs_recheckorderby)
        {
            econtext->ecxt_scantuple = slot;
            ResetExprContext(econtext);
            EvalOrderByExpressions(node, econtext);

            // Compare index ORDER BY values with recalculated values
            cmp = cmp_orderbyvals(node->iss_OrderByValues, node->iss_OrderByNulls,
                                  scandesc->xs_orderbyvals, scandesc->xs_orderbynulls,
                                  node);
            if (cmp < 0)
                elog(ERROR, "index returned tuples in wrong order");

            was_exact = (cmp == 0);
            lastfetched_vals = node->iss_OrderByValues;
            lastfetched_nulls = node->iss_OrderByNulls;
        }
        else
        {
            was_exact = true;
            lastfetched_vals = scandesc->xs_orderbyvals;
            lastfetched_nulls = scandesc->xs_orderbynulls;
        }

        // Decide whether to queue tuple or return it immediately
        if (!was_exact || (topmost && cmp_orderbyvals(lastfetched_vals, lastfetched_nulls,
                                                      topmost->orderbyvals, topmost->orderbynulls,
                                                      node) > 0))
        {
            // Queue tuple for proper ordering
            reorderqueue_push(node, slot, lastfetched_vals, lastfetched_nulls);
            continue;
        }
        else
        {
            // Can return immediately
            return slot;
        }
    }

    return ExecClearTuple(slot);
}
```