# FunctionNext

## Location
[src/backend/executor/nodeFunctionscan.c:59-248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeFunctionscan.c#L59-L248)

## Overview
FunctionNext is the core workhorse function for ExecFunctionScan that retrieves the next tuple from table functions in a function scan operation.

## Definition

```c
structed the
			 * tuplestore itself, didn't leave it pointing at the start. This
			 * call is fast, so the overhead shouldn't be an issue.
			 */
			tuplestore_rescan(tstore);
```
## Detailed Description
FunctionNext implements the tuple retrieval logic for function scans in PostgreSQL's executor. It handles both simple and complex function scan scenarios:

1. **Simple Path**: When the function return type matches the scan result type, it directly fetches results into the scan slot for optimal performance.

2. **Complex Path**: For multiple functions or type mismatches, it:
   - Manages ordinal counters for positioning
   - Iterates through all functions in the function list
   - Copies values from function slots to the scan slot
   - Handles NULL padding for functions that return fewer rows
   - Adds ordinality columns when requested

The function uses tuplestores to cache function results, allowing for efficient forward and backward scanning. It properly handles end-of-data conditions and maintains row count information for backward scan support.

## Parameters / Member Variables
- : FunctionScanState containing the scan state information, function states, tuple slots, and execution flags

## Dependencies
- Functions called/Symbols referenced:
  - [ExecMakeTableFunctionResult](../E/ExecMakeTableFunctionResult.md)
  - [tuplestore_rescan](../t/tuplestore_rescan.md)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [slot_getallattrs](../s/slot_getallattrs.md)
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
  - ScanDirectionIsForward
  - TupIsNull
  - Int64GetDatumFast
- Called from (representative examples):
  - [ExecFunctionScan](../E/ExecFunctionScan.md)

## Notes and Other Information
- Implements both forward and backward scanning capabilities
- Uses tuplestore caching to avoid re-executing functions on subsequent calls
- Handles mixed row counts from multiple functions by padding shorter results with NULLs
- Maintains accurate ordinal positioning for ORDINALITY columns
- Optimizes the common single-function case with a fast path
- Supports the EXEC_FLAG_BACKWARD execution flag for backward-compatible scans

## Simplified Source

```c
static TupleTableSlot *
FunctionNext(FunctionScanState *node)
{
    EState *estate = node->ss.ps.state;
    ScanDirection direction = estate->es_direction;
    TupleTableSlot *scanslot = node->ss.ss_ScanTupleSlot;

    // Fast path for simple case: single function with matching return type
    if (node->simple) {
        Tuplestorestate *tstore = node->funcstates[0].tstore;

        // Initialize tuplestore on first call by executing function
        if (tstore == NULL) {
            node->funcstates[0].tstore = tstore =
                ExecMakeTableFunctionResult(node->funcstates[0].setexpr,
                                            node->ss.ps.ps_ExprContext,
                                            node->argcontext,
                                            node->funcstates[0].tupdesc,
                                            node->eflags & EXEC_FLAG_BACKWARD);
            tuplestore_rescan(tstore);
        }

        // Fetch next tuple from tuplestore
        tuplestore_gettupleslot(tstore, ScanDirectionIsForward(direction), false, scanslot);
        return scanslot;
    }

    // Complex path: multiple functions or type conversions needed

    // Update ordinal counter for positioning
    int64 oldpos = node->ordinal;
    if (ScanDirectionIsForward(direction))
        node->ordinal++;
    else
        node->ordinal--;

    // Clear result slot and process all functions
    ExecClearTuple(scanslot);
    int att = 0;
    bool alldone = true;

    for (int funcno = 0; funcno < node->nfuncs; funcno++) {
        FunctionScanPerFuncState *fs = &node->funcstates[funcno];

        // Initialize tuplestore for this function if needed
        if (fs->tstore == NULL) {
            fs->tstore = ExecMakeTableFunctionResult(fs->setexpr,
                                                     node->ss.ps.ps_ExprContext,
                                                     node->argcontext,
                                                     fs->tupdesc,
                                                     node->eflags & EXEC_FLAG_BACKWARD);
            tuplestore_rescan(fs->tstore);
        }

        // Get next tuple from this function's tuplestore
        if (fs->rowcount != -1 && fs->rowcount < oldpos) {
            ExecClearTuple(fs->func_slot);
        } else {
            tuplestore_gettupleslot(fs->tstore, ScanDirectionIsForward(direction),
                                    false, fs->func_slot);
        }

        if (TupIsNull(fs->func_slot)) {
            // Function exhausted - record row count and pad with NULLs
            if (ScanDirectionIsForward(direction) && fs->rowcount == -1)
                fs->rowcount = node->ordinal;

            for (int i = 0; i < fs->colcount; i++) {
                scanslot->tts_values[att] = (Datum) 0;
                scanslot->tts_isnull[att] = true;
                att++;
            }
        } else {
            // Copy function result to scan slot
            slot_getallattrs(fs->func_slot);
            for (int i = 0; i < fs->colcount; i++) {
                scanslot->tts_values[att] = fs->func_slot->tts_values[i];
                scanslot->tts_isnull[att] = fs->func_slot->tts_isnull[i];
                att++;
            }
            alldone = false;
        }
    }

    // Add ordinality column if requested
    if (node->ordinality) {
        scanslot->tts_values[att] = Int64GetDatumFast(node->ordinal);
        scanslot->tts_isnull[att] = false;
    }

    // Finalize virtual tuple if we have data
    if (!alldone)
        ExecStoreVirtualTuple(scanslot);

    return scanslot;
}
```