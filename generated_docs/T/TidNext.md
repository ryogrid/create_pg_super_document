# TidNext

## Location
[src/backend/executor/nodeTidscan.c:312-402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidscan.c#L312-L402)

## Overview
TidNext is a static function that retrieves the next tuple from a TID scan by iterating through a pre-computed list of TIDs and fetching the corresponding tuples from the relation.

## Definition
```c
static TupleTableSlot *TidNext(TidScanState *node)
```

## Detailed Description
This function implements the core tuple retrieval logic for TID scans. It manages the iteration through a list of TIDs (computed by TidListEval) and fetches the actual tuples from the heap relation. The function supports both forward and backward scan directions.

Key operational aspects:
1. **Lazy initialization**: On first call, invokes TidListEval to compute the TID list
2. **Bidirectional scanning**: Supports both forward and backward scan directions via scan direction parameter
3. **Position management**: Maintains tss_TidPtr as the current position in the TID array
4. **CURRENT OF handling**: For cursor-based operations, retrieves the latest version of tuples using table_tuple_get_latest_tid
5. **Snapshot compliance**: Uses table_tuple_fetch_row_version to ensure snapshot visibility
6. **Error handling**: Gracefully handles invalid TIDs and snapshot qualification failures

The function continues iterating through TIDs until it finds a valid, visible tuple or exhausts the TID list. For each TID, it attempts to fetch the tuple version that satisfies the query's snapshot. If a TID is invalid or the tuple doesn't meet snapshot criteria, it advances to the next TID.

## Parameters / Member Variables
- `node`: Pointer to TidScanState structure containing the scan state, TID list, current position, and other scan-related information

## Dependencies
- Functions called/Symbols referenced:
  - [TidListEval](TidListEval.md)
  - ScanDirectionIsBackward
  - [table_tuple_get_latest_tid](../t/table_tuple_get_latest_tid.md)
  - [table_tuple_fetch_row_version](../t/table_tuple_fetch_row_version.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - CHECK_FOR_INTERRUPTS
- Types used:
  - [TidScanState](TidScanState.md)
  - [TupleTableSlot](TupleTableSlot.md)
  - [EState](../E/EState.md)
  - ScanDirection
  - [Snapshot](../S/Snapshot.md)
  - [TableScanDesc](TableScanDesc.md)
  - [Relation](../R/Relation.md)
  - [ItemPointerData](../I/ItemPointerData.md)
- Called from:
  - [ExecTidScan](../E/ExecTidScan.md)

## Notes and Other Information
- This is a static function, only accessible within nodeTidscan.c
- Implements lazy evaluation - TID list is computed only when first tuple is requested
- Supports query cancellation via CHECK_FOR_INTERRUPTS() macro
- Handles both regular TID expressions and CURRENT OF cursor expressions
- The function is stateful - maintains position (tss_TidPtr) between calls
- Returns NULL (via ExecClearTuple) when no more tuples are available
- Part of PostgreSQL's executor framework for direct tuple access via TID values
- Optimized for sequential access through the sorted TID list produced by TidListEval

## Simplified Source

```c
static TupleTableSlot *
TidNext(TidScanState *node)
{
    EState *estate = node->ss.ps.state;
    ScanDirection direction = estate->es_direction;
    Snapshot snapshot = estate->es_snapshot;
    Relation heapRelation = node->ss.ss_currentRelation;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

    // First time: compute TID list
    if (node->tss_TidList == NULL)
        TidListEval(node);

    TableScanDesc scan = node->ss.ss_currentScanDesc;
    ItemPointerData *tidList = node->tss_TidList;
    int numTids = node->tss_NumTids;

    // Initialize or advance scan position
    bool bBackward = ScanDirectionIsBackward(direction);
    if (bBackward) {
        if (node->tss_TidPtr < 0)
            node->tss_TidPtr = numTids - 1;  // init backward
        else
            node->tss_TidPtr--;
    } else {
        if (node->tss_TidPtr < 0)
            node->tss_TidPtr = 0;  // init forward
        else
            node->tss_TidPtr++;
    }

    // Iterate through TID list to find valid tuple
    while (node->tss_TidPtr >= 0 && node->tss_TidPtr < numTids) {
        ItemPointerData tid = tidList[node->tss_TidPtr];

        // Handle CURRENT OF cursor case
        if (node->tss_isCurrentOf)
            table_tuple_get_latest_tid(scan, &tid);

        // Try to fetch tuple version
        if (table_tuple_fetch_row_version(heapRelation, &tid, snapshot, slot))
            return slot;

        // Bad TID or failed snapshot - try next
        if (bBackward)
            node->tss_TidPtr--;
        else
            node->tss_TidPtr++;

        CHECK_FOR_INTERRUPTS();
    }

    // End of scan
    return ExecClearTuple(slot);
}
```