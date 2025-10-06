# hypothetical_dense_rank_final

## Location
[src/backend/utils/adt/orderedsetaggs.c:1295-1430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L1295-L1430)

## Overview
Implements the SQL dense rank function for hypothetical rows in ordered-set aggregates, calculating the dense rank (rank without gaps) of where a hypothetical row would appear in a dataset.

## Definition
```c
Datum hypothetical_dense_rank_final(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the final phase of the dense_rank() ordered-set aggregate function for hypothetical rows. Unlike regular rank which can have gaps when there are ties, dense_rank provides consecutive ranking numbers by eliminating gaps. The function calculates where a hypothetical row would rank in an ordered dataset, but ensures that ranks are consecutive (1, 2, 3, ...) even when there are duplicate values.

The implementation differs significantly from the simpler percent_rank and cume_dist functions. It performs a complete sort of the data with the hypothetical row inserted, then iterates through all rows to count duplicates and calculate the proper dense rank. It uses tuple comparison logic to identify duplicate rows and subtract the duplicate count from the final rank to eliminate gaps.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the aggregate state and hypothetical row values
- Local variables:
  - `rank`: Running count of position, starts at 1
  - `duplicate_count`: Number of duplicate values encountered before the hypothetical row
  - `econtext`: Expression context for tuple comparisons
  - `compareTuple`: Compiled expression for comparing tuple equality
  - `slot`, `slot2`, `extraslot`: Tuple table slots for managing row data during iteration

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md): Validates aggregate function call context
  - [CreateStandaloneExprContext](../C/CreateStandaloneExprContext.md): Creates expression evaluation context
  - [hypothetical_check_argtypes](hypothetical_check_argtypes.md): Validates argument types for hypothetical functions
  - [execTuplesMatchPrepare](../e/execTuplesMatchPrepare.md): Prepares tuple comparison expression
  - [ExecClearTuple](../E/ExecClearTuple.md), ExecStoreVirtualTuple: Tuple slot manipulation functions
  - [tuplesort_puttupleslot](../t/tuplesort_puttupleslot.md), tuplesort_performsort, tuplesort_gettupleslot: Tuple sorting operations
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md), ExecDropSingleTupleTableSlot: Tuple slot lifecycle management
  - [ExecQualAndReset](../E/ExecQualAndReset.md): Execute tuple comparison and reset expression state
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's aggregate function dispatch mechanism)

## Notes and Other Information
- This is the most complex of the hypothetical ranking functions due to the need to eliminate ranking gaps
- Uses a flag value of -1 when inserting the hypothetical row to sort it ahead of equal peers
- Performs tuple-by-tuple comparison using PostgreSQL's expression evaluation system to identify duplicates
- The algorithm alternates between two tuple slots to maintain access to the previous row for comparison
- Returns int64 values representing dense ranks (1, 2, 3, ... without gaps)
- Used in SQL queries like `SELECT dense_rank(value) WITHIN GROUP (ORDER BY column) FROM table`
- More computationally expensive than regular rank functions due to duplicate detection requirements
- Located in src/backend/utils/adt/orderedsetaggs.c:1295-1430

## Simplified Source
```c
Datum hypothetical_dense_rank_final(PG_FUNCTION_ARGS) {
    int nargs = PG_NARGS() - 1;
    int64 rank = 1, duplicate_count = 0;
    OSAPerGroupState *osastate;
    ExprContext *econtext;
    ExprState *compareTuple;
    TupleTableSlot *slot, *slot2, *extraslot;

    // Return rank 1 if no regular rows exist
    if (PG_ARGISNULL(0))
        PG_RETURN_INT64(rank);

    osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

    // Set up expression context if not already done
    econtext = osastate->qstate->econtext;
    if (!econtext) {
        osastate->qstate->econtext = CreateStandaloneExprContext();
        econtext = osastate->qstate->econtext;
    }

    // Validate arguments (must be even - direct + aggregated pairs)
    if (nargs % 2 != 0)
        elog(ERROR, "wrong number of arguments in hypothetical-set function");
    nargs /= 2;

    hypothetical_check_argtypes(fcinfo, nargs, osastate->qstate->tupdesc);

    // Build tuple comparator for detecting duplicates
    compareTuple = osastate->qstate->compareTuple;
    if (compareTuple == NULL) {
        compareTuple = execTuplesMatchPrepare(osastate->qstate->tupdesc,
                                              nargs, /* numDistinctCols */
                                              osastate->qstate->sortColIdx,
                                              osastate->qstate->eqOperators,
                                              osastate->qstate->sortCollations,
                                              NULL);
        osastate->qstate->compareTuple = compareTuple;
    }

    // Insert hypothetical row with flag -1 for dense ranking
    slot = osastate->qstate->tupslot;
    ExecClearTuple(slot);
    for (int i = 0; i < nargs; i++) {
        slot->tts_values[i] = PG_GETARG_DATUM(i + 1);
        slot->tts_isnull[i] = PG_ARGISNULL(i + 1);
    }
    slot->tts_values[nargs] = Int32GetDatum(-1);
    slot->tts_isnull[nargs] = false;
    ExecStoreVirtualTuple(slot);

    tuplesort_puttupleslot(osastate->sortstate, slot);
    tuplesort_performsort(osastate->sortstate);
    osastate->sort_done = true;

    // Use two slots to compare consecutive tuples
    extraslot = MakeSingleTupleTableSlot(osastate->qstate->tupdesc, &TTSOpsMinimalTuple);
    slot2 = extraslot;

    // Scan until hypothetical row, counting duplicates
    while (tuplesort_gettupleslot(osastate->sortstate, true, true, slot, NULL)) {
        bool isnull;
        Datum flag_value = slot_getattr(slot, nargs + 1, &isnull);

        // Stop when we find hypothetical row (non-zero flag)
        if (!isnull && DatumGetInt32(flag_value) != 0)
            break;

        // Check if current row duplicates previous row
        econtext->ecxt_outertuple = slot;
        econtext->ecxt_innertuple = slot2;

        if (!TupIsNull(slot2) && ExecQualAndReset(compareTuple, econtext))
            duplicate_count++;

        // Swap slots for next iteration
        TupleTableSlot *tmpslot = slot2;
        slot2 = slot;
        slot = tmpslot;
        rank++;
    }

    ExecDropSingleTupleTableSlot(extraslot);

    // Dense rank = rank - duplicates (eliminates gaps)
    rank = rank - duplicate_count;

    PG_RETURN_INT64(rank);
}
```