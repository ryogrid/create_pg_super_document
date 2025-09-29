# ExecReScanFunctionScan

## Location
[src/backend/executor/nodeFunctionscan.c:556-613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeFunctionscan.c#L556-L613)

## Overview
Rescans a FunctionScan node by either recomputing function outputs or rewinding existing tuplestores, depending on whether parameters have changed.

## Definition
void ExecReScanFunctionScan(FunctionScanState *node)

## Detailed Description
ExecReScanFunctionScan implements the rescan functionality for function scan nodes in PostgreSQL's executor. When a rescan is triggered, this function determines whether to drop existing tuplestores and recompute function outputs or simply rewind the existing tuplestores. The decision is based on whether any parameters referenced by the functions have changed.

The function performs several key operations:
1. Clears result tuple slots for the node and all function states
2. Calls the generic scan rescan functionality
3. Checks if any function parameters have changed using the chgparam bitmap
4. For functions with changed parameters, destroys existing tuplestores to force recomputation
5. Resets the ordinality counter for functions that return ordinal positions
6. Rewinds any remaining tuplestores that don't need recomputation

This approach optimizes rescans by avoiding unnecessary recomputation when function parameters haven't changed, while ensuring correctness when they have.

## Parameters / Member Variables
- : FunctionScanState pointer containing the execution state for the function scan node, including function states, tuple slots, and parameter change information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecClearTuple](ExecClearTuple.md)
  - [ExecScanReScan](ExecScanReScan.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [tuplestore_end](../t/tuplestore_end.md)
  - [tuplestore_rescan](../t/tuplestore_rescan.md)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (src/backend/executor/execAmi.c:209)

## Notes and Other Information
- The function includes a design comment noting that it could potentially recompute volatile functions on every rescan, but follows the general executor pattern of not conditionalizing actions based on function volatility
- The ordinality counter reset ensures proper row numbering for functions that include ordinal positions in their output
- Parameter change detection uses bitmapset operations to efficiently determine if function-specific parameters have been modified
- Tuplestores are PostgreSQL's mechanism for storing intermediate results from set-returning functions

## Simplified Source

```c
void ExecReScanFunctionScan(FunctionScanState *node) {
    FunctionScan *scan = (FunctionScan *) node->ss.ps.plan;
    Bitmapset *chgparam = node->ss.ps.chgParam;

    // Clear result tuple slot
    if (node->ss.ps.ps_ResultTupleSlot) {
        ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    }

    // Clear all function tuple slots
    for (int i = 0; i < node->nfuncs; i++) {
        FunctionScanPerFuncState *fs = &node->funcstates[i];
        if (fs->func_slot) {
            ExecClearTuple(fs->func_slot);
        }
    }

    // Reset scan state
    ExecScanReScan(&node->ss);

    // Check if any function parameters changed
    if (chgparam) {
        ListCell *lc;
        int i = 0;

        // For each function, check if its parameters changed
        foreach(lc, scan->functions) {
            RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

            // If function parameters changed, drop tuplestore to force recomputation
            if (bms_overlap(chgparam, rtfunc->funcparams)) {
                if (node->funcstates[i].tstore != NULL) {
                    tuplestore_end(node->funcstates[i].tstore);
                    node->funcstates[i].tstore = NULL;
                }
                node->funcstates[i].rowcount = -1;
            }
            i++;
        }
    }

    // Reset ordinality counter
    node->ordinal = 0;

    // Rewind remaining tuplestores that don't need recomputation
    for (int i = 0; i < node->nfuncs; i++) {
        if (node->funcstates[i].tstore != NULL) {
            tuplestore_rescan(node->funcstates[i].tstore);
        }
    }
}
```