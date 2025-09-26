# process_ordered_aggregate_multi

## Location
[src/backend/executor/nodeAgg.c:949-1045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L949-L1045)

## Overview
Processes ordered aggregates with multiple input columns by completing the sort, reading tuples in sorted order, and applying the transition function while handling DISTINCT logic across multiple columns.

## Definition
```c
static void process_ordered_aggregate_multi(AggState *aggstate,
                                          AggStatePerTrans pertrans,
                                          AggStatePerGroup pergroupstate)
```

## Detailed Description
This function handles the execution phase of DISTINCT or ORDER BY aggregates that have multiple input columns. It is called after all input values have been entered into the sort object during the scanning phase. The function completes the sort operation, reads out tuples in sorted order, and applies the aggregate's transition function to each distinct tuple.

Key behaviors and optimizations:
- Uses tuple-based sorting (tuplesort_gettupleslot) instead of datum-based sorting for handling multiple columns
- Implements DISTINCT logic by comparing consecutive sorted tuples using multi-column equality expressions
- Efficiently manages two TupleTableSlots for DISTINCT comparisons by swapping pointers to avoid tuple copying
- Extracts only the required number of transition input columns from each tuple to pass to the transition function
- Uses abbreviated comparison keys when available to optimize DISTINCT checks
- Carefully manages expression context state, including saving and restoring outer tuple references

The function coordinates multiple components: tuple sorting, slot management, expression evaluation for equality checks, and transition function advancement.

## Parameters / Member Variables
- `aggstate`: Main aggregate state containing current grouping set information, memory contexts, and temporary expression context
- `pertrans`: Per-transition state containing sort states, tuple slots (sortslot, uniqslot), function call info, and multi-column equality expressions
- `pergroupstate`: Per-group state where transition values are stored and updated

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_performsort](../t/tuplesort_performsort.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [tuplesort_gettupleslot](../t/tuplesort_gettupleslot.md)
  - [ExecQual](../E/ExecQual.md)
  - [slot_getsomeattrs](../s/slot_getsomeattrs.md)
  - [advance_transition_function](../a/advance_transition_function.md)
  - ResetExprContext
  - [tuplesort_end](../t/tuplesort_end.md)
- Data types used:
  - [AggState](../A/AggState.md)
  - [AggStatePerTrans](../A/AggStatePerTrans.md)
  - [AggStatePerGroup](../A/AggStatePerGroup.md)
  - [ExprContext](../E/ExprContext.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
  - [TupleTableSlot](../T/TupleTableSlot.md)
- Called from (representative examples):
  - [finalize_aggregates](../f/finalize_aggregates.md)

## Notes and Other Information
- Handles only one grouping set (already set in aggstate->current_set) per invocation
- Expects CurrentMemoryContext to be the per-query context when called
- Uses slot pointer swapping optimization to retain current tuple for next comparison without copying
- Includes CHECK_FOR_INTERRUPTS() to handle query cancellation during long-running sorts
- Properly manages expression context by saving and restoring ecxt_outertuple to avoid conflicts with grouping sets
- The multi-column equality check is performed using ExecQual with pertrans->equalfnMulti expression
- Cleans up tuple slots and sort state when finished, setting sort state pointer to NULL
- More complex than the single-column case but provides necessary functionality for multi-column DISTINCT and ORDER BY aggregates