# eval_windowaggregates

## Location
[src/backend/executor/nodeWindowAgg.c:663-1032](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L663-L1032)

## Overview
Evaluates plain aggregates being used as window functions, managing frame boundaries and optimizing computation through incremental updates and inverse transitions.

## Definition

```c
static void
eval_windowaggregates(WindowAggState *winstate)
```
## Detailed Description
This is the core function for evaluating window aggregates and differs significantly from nodeAgg.c in two key ways: it uses inverse transition functions to remove rows when the window frame start moves, and it supports calling aggregate final functions repeatedly on the same transition value. The function implements sophisticated optimizations including incremental aggregation for contiguous frames, frame reuse when successive rows share identical frames, and selective restart strategies. It handles complex frame semantics including exclusion clauses, manages memory contexts carefully, and coordinates between forward aggregation (via ) and backward removal (via ).

## Parameters / Member Variables
- : The complete window aggregate execution state containing all per-function and per-aggregate states, frame positions, memory contexts, and optimization flags

## Dependencies
- Functions called/Symbols referenced:
  - [update_frameheadpos](../u/update_frameheadpos.md)
  - [window_gettupleslot](../w/window_gettupleslot.md)
  - [advance_windowaggregate_base](../a/advance_windowaggregate_base.md)
  - ResetExprContext
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [WinSetMarkPosition](../W/WinSetMarkPosition.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [initialize_windowaggregate](../i/initialize_windowaggregate.md)
  - TupIsNull
  - [row_is_in_frame](../r/row_is_in_frame.md)
  - [advance_windowaggregate](../a/advance_windowaggregate.md)
  - [finalize_windowaggregate](../f/finalize_windowaggregate.md)
  - [datumCopy](../d/datumCopy.md)
- Called from (representative examples):
  - [ExecWindowAgg](../E/ExecWindowAgg.md)

## Notes and Other Information
- Implements multiple optimization strategies: incremental aggregation for UNBOUNDED_PRECEDING frames, inverse transitions for moving frames, and frame result reuse for identical frames
- Handles restart conditions: first row in partition, frame head movement without inverse functions, exclusion clauses, or non-overlapping frames
- Manages  and  pointers to track which rows have been processed and which need processing
- For moving frames, attempts to use inverse transition functions to remove rows that fall out of the frame, falling back to full restart if inverse transitions fail
- Supports shared and private aggregate memory contexts with different cleanup strategies
- Maintains loop invariant that  is either empty or contains the row at 
- Saves aggregate results in per-aggregate state to enable frame result reuse for subsequent rows with identical frames
- Handles exclusion clauses by punting to full recalculation for every row (optimization opportunity for contiguous exclusions)
- Frame end position is validated to never move backwards to ensure correctness

## Simplified Source

```c
static void
eval_windowaggregates(WindowAggState *winstate)
{
    WindowStatePerAgg peraggstate;
    int numaggs = winstate->numaggs;
    int numaggs_restart = 0;
    int64 aggregatedupto_nonrestarted;
    ExprContext *econtext = winstate->ss.ps.ps_ExprContext;
    WindowObject agg_winobj = winstate->agg_winobj;
    TupleTableSlot *agg_row_slot = winstate->agg_row_slot;
    TupleTableSlot *temp_slot = winstate->temp_slot_1;

    if (numaggs == 0)
        return;  // No aggregates to process

    // Update frame head position first
    update_frameheadpos(winstate);
    if (winstate->frameheadpos < winstate->aggregatedbase)
        elog(ERROR, "window frame head moved backward");

    // Check if we can reuse previous results (optimization for identical frames)
    if (winstate->aggregatedbase == winstate->frameheadpos &&
        (winstate->frameOptions & (FRAMEOPTION_END_UNBOUNDED_FOLLOWING | FRAMEOPTION_END_CURRENT_ROW)) &&
        !(winstate->frameOptions & FRAMEOPTION_EXCLUSION) &&
        winstate->aggregatedbase <= winstate->currentpos &&
        winstate->aggregatedupto > winstate->currentpos) {

        // Reuse cached results
        for (int i = 0; i < numaggs; i++) {
            peraggstate = &winstate->peragg[i];
            int wfuncno = peraggstate->wfuncno;
            econtext->ecxt_aggvalues[wfuncno] = peraggstate->resultValue;
            econtext->ecxt_aggnulls[wfuncno] = peraggstate->resultValueIsNull;
        }
        return;
    }

    // Determine which aggregates need restart
    for (int i = 0; i < numaggs; i++) {
        peraggstate = &winstate->peragg[i];
        if (winstate->currentpos == 0 ||
            (winstate->aggregatedbase != winstate->frameheadpos && !OidIsValid(peraggstate->invtransfn_oid)) ||
            (winstate->frameOptions & FRAMEOPTION_EXCLUSION) ||
            winstate->aggregatedupto <= winstate->frameheadpos) {
            peraggstate->restart = true;
            numaggs_restart++;
        } else {
            peraggstate->restart = false;
        }
    }

    // Try to use inverse transitions to remove rows that fell out of frame
    while (numaggs_restart < numaggs && winstate->aggregatedbase < winstate->frameheadpos) {
        // Fetch row being removed
        if (!window_gettupleslot(agg_winobj, winstate->aggregatedbase, temp_slot))
            elog(ERROR, "could not re-fetch previously fetched frame row");

        winstate->tmpcontext->ecxt_outertuple = temp_slot;

        // Apply inverse transitions for non-restarting aggregates
        for (int i = 0; i < numaggs; i++) {
            peraggstate = &winstate->peragg[i];
            if (peraggstate->restart)
                continue;

            int wfuncno = peraggstate->wfuncno;
            bool ok = advance_windowaggregate_base(winstate, &winstate->perfunc[wfuncno], peraggstate);
            if (!ok) {
                // Inverse transition failed, mark for restart
                peraggstate->restart = true;
                numaggs_restart++;
            }
        }

        ResetExprContext(winstate->tmpcontext);
        winstate->aggregatedbase++;
        ExecClearTuple(temp_slot);
    }

    winstate->aggregatedbase = winstate->frameheadpos;

    // Update frame head mark pointer
    if (agg_winobj->markptr >= 0)
        WinSetMarkPosition(agg_winobj, winstate->frameheadpos);

    // Restart aggregates that need it
    if (numaggs_restart > 0)
        MemoryContextReset(winstate->aggcontext);

    for (int i = 0; i < numaggs; i++) {
        peraggstate = &winstate->peragg[i];
        if (peraggstate->restart) {
            int wfuncno = peraggstate->wfuncno;
            initialize_windowaggregate(winstate, &winstate->perfunc[wfuncno], peraggstate);
        } else if (!peraggstate->resultValueIsNull) {
            // Free old non-null result
            if (!peraggstate->resulttypeByVal)
                pfree(DatumGetPointer(peraggstate->resultValue));
            peraggstate->resultValue = (Datum) 0;
            peraggstate->resultValueIsNull = true;
        }
    }

    // Set starting position for aggregation
    aggregatedupto_nonrestarted = winstate->aggregatedupto;
    if (numaggs_restart > 0 && winstate->aggregatedupto != winstate->frameheadpos) {
        winstate->aggregatedupto = winstate->frameheadpos;
        ExecClearTuple(agg_row_slot);
    }

    // Process rows in frame
    for (;;) {
        // Fetch next row if needed
        if (TupIsNull(agg_row_slot)) {
            if (!window_gettupleslot(agg_winobj, winstate->aggregatedupto, agg_row_slot))
                break;  // End of partition
        }

        // Check if row is in frame
        int ret = row_is_in_frame(winstate, winstate->aggregatedupto, agg_row_slot);
        if (ret < 0)
            break;  // No more rows in frame
        if (ret == 0)
            goto next_tuple;  // Row not in frame, but continue

        // Set tuple context for aggregate evaluation
        winstate->tmpcontext->ecxt_outertuple = agg_row_slot;

        // Process row through all aggregates
        for (int i = 0; i < numaggs; i++) {
            peraggstate = &winstate->peragg[i];

            // Skip non-restarted aggregates until catch-up point
            if (!peraggstate->restart && winstate->aggregatedupto < aggregatedupto_nonrestarted)
                continue;

            int wfuncno = peraggstate->wfuncno;
            advance_windowaggregate(winstate, &winstate->perfunc[wfuncno], peraggstate);
        }

next_tuple:
        ResetExprContext(winstate->tmpcontext);
        winstate->aggregatedupto++;
        ExecClearTuple(agg_row_slot);
    }

    // Finalize aggregates and store results
    for (int i = 0; i < numaggs; i++) {
        peraggstate = &winstate->peragg[i];
        int wfuncno = peraggstate->wfuncno;
        Datum *result = &econtext->ecxt_aggvalues[wfuncno];
        bool *isnull = &econtext->ecxt_aggnulls[wfuncno];

        finalize_windowaggregate(winstate, &winstate->perfunc[wfuncno], peraggstate, result, isnull);

        // Cache result for potential reuse
        if (!peraggstate->resulttypeByVal && !*isnull) {
            MemoryContext oldContext = MemoryContextSwitchTo(peraggstate->aggcontext);
            peraggstate->resultValue = datumCopy(*result, peraggstate->resulttypeByVal, peraggstate->resulttypeLen);
            MemoryContextSwitchTo(oldContext);
        } else {
            peraggstate->resultValue = *result;
        }
        peraggstate->resultValueIsNull = *isnull;
    }
}
```