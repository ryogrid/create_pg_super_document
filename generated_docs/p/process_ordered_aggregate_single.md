# process_ordered_aggregate_single

## Location
[src/backend/executor/nodeAgg.c:848-948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L848-L948)

## Overview
Processes ordered aggregates with a single input column by completing the sort, reading values in sorted order, and applying the transition function while handling DISTINCT logic.

## Definition
```c
static void process_ordered_aggregate_single(AggState *aggstate,
                                           AggStatePerTrans pertrans,
                                           AggStatePerGroup pergroupstate)
```

## Detailed Description
This function handles the execution phase of DISTINCT or ORDER BY aggregates that have only one input column. It is called after all input values have been entered into the sort object during the scanning phase. The function completes the sort operation, reads out values in sorted order, and applies the aggregate's transition function to each value.

Key optimizations and behaviors:
- Separated from multi-input case for performance: single by-value inputs (like count(distinct id)) run ~300% faster using tuplesort_getdatum
- Implements SQL DISTINCT logic by comparing consecutive sorted values using equality functions
- Manages memory carefully for pass-by-reference types, ensuring proper cleanup of temporary values
- Uses abbreviated comparison when available to optimize DISTINCT checks
- Handles both by-value and by-reference input types appropriately

The function assumes the transition function strictness was already validated during input collection, so it focuses on DISTINCT filtering and value processing.

## Parameters / Member Variables
- `aggstate`: Main aggregate state containing current grouping set information and memory contexts
- `pertrans`: Per-transition state containing sort states, function call info, type information, and equality functions
- `pergroupstate`: Per-group state where transition values are stored and updated

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_performsort](../t/tuplesort_performsort.md)
  - [tuplesort_getdatum](../t/tuplesort_getdatum.md)  
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [advance_transition_function](../a/advance_transition_function.md)
  - [datumCopy](../d/datumCopy.md)
  - [tuplesort_end](../t/tuplesort_end.md)
- Data types used:
  - [AggState](../A/AggState.md)
  - [AggStatePerTrans](../A/AggStatePerTrans.md)
  - [AggStatePerGroup](../A/AggStatePerGroup.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
- Called from (representative examples):
  - [finalize_aggregates](../f/finalize_aggregates.md)

## Notes and Other Information
- Designed specifically for single-column ordered aggregates to maximize performance
- Expects CurrentMemoryContext to be the per-query context when called
- Handles only one grouping set (already set in aggstate->current_set)
- The tuplesort_getdatum path provides significant performance benefits over the general multi-input case
- Properly manages memory for pass-by-reference datums returned by the sort, which are palloc'd in per-query context
- Uses abbreviated values when available for faster DISTINCT comparisons
- Cleans up the sort state by calling tuplesort_end and setting the pointer to NULL when finished

## Simplified Source

```c
static void
process_ordered_aggregate_single(AggState *aggstate,
                                 AggStatePerTrans pertrans,
                                 AggStatePerGroup pergroupstate)
{
    Datum oldVal = (Datum) 0;
    bool oldIsNull = true;
    bool haveOldVal = false;
    MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
    MemoryContext oldContext;
    bool isDistinct = (pertrans->numDistinctCols > 0);
    Datum newAbbrevVal = (Datum) 0;
    Datum oldAbbrevVal = (Datum) 0;
    FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
    Datum *newVal;
    bool *isNull;

    // Complete the sort operation
    tuplesort_performsort(pertrans->sortstates[aggstate->current_set]);

    // Setup pointers to function call arguments
    newVal = &fcinfo->args[1].value;
    isNull = &fcinfo->args[1].isnull;

    // Process sorted values
    while (tuplesort_getdatum(pertrans->sortstates[aggstate->current_set],
                              true, false, newVal, isNull, &newAbbrevVal))
    {
        // Switch to working context for comparisons
        MemoryContextReset(workcontext);
        oldContext = MemoryContextSwitchTo(workcontext);

        // Skip duplicate values for DISTINCT aggregates
        if (isDistinct &&
            haveOldVal &&
            ((oldIsNull && *isNull) ||
             (!oldIsNull && !*isNull &&
              oldAbbrevVal == newAbbrevVal &&
              DatumGetBool(FunctionCall2Coll(&pertrans->equalfnOne,
                                           pertrans->aggCollation,
                                           oldVal, *newVal)))))
        {
            MemoryContextSwitchTo(oldContext);
            continue;
        }
        else
        {
            // Apply transition function
            advance_transition_function(aggstate, pertrans, pergroupstate);
            MemoryContextSwitchTo(oldContext);

            // Update old value for next comparison
            if (!pertrans->inputtypeByVal)
            {
                // Cleanup previous by-reference value
                if (!oldIsNull)
                    pfree(DatumGetPointer(oldVal));
                // Copy new by-reference value
                if (!*isNull)
                    oldVal = datumCopy(*newVal, pertrans->inputtypeByVal,
                                     pertrans->inputtypeLen);
            }
            else
                oldVal = *newVal;

            oldAbbrevVal = newAbbrevVal;
            oldIsNull = *isNull;
            haveOldVal = true;
        }
    }

    // Final cleanup of by-reference value
    if (!oldIsNull && !pertrans->inputtypeByVal)
        pfree(DatumGetPointer(oldVal));

    // Cleanup sort state
    tuplesort_end(pertrans->sortstates[aggstate->current_set]);
    pertrans->sortstates[aggstate->current_set] = NULL;
}
```