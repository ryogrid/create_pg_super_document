# fetch_input_tuple

## Location
[src/backend/executor/nodeAgg.c:547-577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L547-L577)

## Overview
Fetches input tuples for aggregate processing from either the outer plan (phase 1) or from a previously sorted tuplesort, optionally copying tuples to the output sorter.

## Definition
```c
static TupleTableSlot *fetch_input_tuple(AggState *aggstate)
```

## Detailed Description
This function serves as the primary input mechanism for multi-phase aggregate operations. It intelligently determines the source of input tuples based on the current execution phase: if a sort_in tuplesort exists (indicating we're in a later phase), it fetches tuples from that sorted input; otherwise, it fetches directly from the outer plan node using ExecProcNode(). Additionally, if an output sorter exists for the next phase, it copies the fetched tuple to that sorter for subsequent processing. The function includes interrupt checking to ensure responsiveness during long-running operations.

## Parameters / Member Variables
- `aggstate`: Pointer to AggState structure containing aggregate execution state

## Dependencies
- Functions called/Symbols referenced:
  - [AggState](../A/AggState.md) (struct type)
  - [tuplesort_gettupleslot](../t/tuplesort_gettupleslot.md)
  - [ExecProcNode](../E/ExecProcNode.md)
  - outerPlanState
  - TupIsNull
  - [tuplesort_puttupleslot](../t/tuplesort_puttupleslot.md)
- Called from (representative examples):
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md)
  - [agg_fill_hash_table](../a/agg_fill_hash_table.md)

## Notes and Other Information
The function includes an important warning in its comment: callers cannot rely on the memory for the returned tuple slot remaining valid past any subsequently fetched tuple. This indicates that the slot may be reused or invalidated by later calls. The function properly handles interrupt checking via CHECK_FOR_INTERRUPTS() when reading from tuplesort, ensuring that long-running sort operations can be cancelled. The dual-path logic (sort_in vs. outer plan) enables efficient multi-phase processing where intermediate results are sorted and passed between phases.

## Simplified Source

```c
static TupleTableSlot *fetch_input_tuple(AggState *aggstate) {
    TupleTableSlot *slot;

    if (aggstate->sort_in) {
        // Get tuple from sorted input (later phases)
        CHECK_FOR_INTERRUPTS();
        if (!tuplesort_gettupleslot(aggstate->sort_in, true, false,
                                    aggstate->sort_slot, NULL))
            return NULL;
        slot = aggstate->sort_slot;
    } else {
        // Get tuple from outer plan (first phase)
        slot = ExecProcNode(outerPlanState(aggstate));
    }

    // Copy to output sorter if needed for next phase
    if (!TupIsNull(slot) && aggstate->sort_out)
        tuplesort_puttupleslot(aggstate->sort_out, slot);

    return slot;
}
```