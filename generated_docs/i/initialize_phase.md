# initialize_phase

## Location
[src/backend/executor/nodeAgg.c:477-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L477-L546)

## Overview
Switches the aggregate execution to a new phase, managing tuplesort states and preparing for the next phase of multi-phase aggregate processing.

## Definition
```c
static void initialize_phase(AggState *aggstate, int newphase)
```

## Detailed Description
This function manages phase transitions in multi-phase aggregate operations, particularly for grouped aggregation with multiple grouping sets. It handles the lifecycle of input and output tuplesorts, ensuring proper cleanup of the previous phase and initialization of the next phase. Phase 0 is reserved for hashing operations (handled last in AGG_MIXED case), while higher phases involve sorting operations. The function performs tuplesort juggling - converting output sorts to input sorts between phases and creating new output sorts as needed.

## Parameters / Member Variables
- `aggstate`: Pointer to AggState structure containing aggregate execution state
- `newphase`: The new phase number to switch to (must be 0, 1, or current_phase + 1)

## Dependencies
- Functions called/Symbols referenced:
  - [AggState](../A/AggState.md) (struct type)
  - tuplesort_end
  - tuplesort_performsort
  - [tuplesort_begin_heap](../t/tuplesort_begin_heap.md)
  - Sort (struct type)
  - outerPlanState
  - [ExecGetResultType](../E/ExecGetResultType.md)
  - TUPLESORT_NONE
- Called from (representative examples):
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md)
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [ExecReScanAgg](../E/ExecReScanAgg.md)

## Notes and Other Information
The function includes strict phase transition validation via Assert() - newphase must be 0 or 1 (for reset) or exactly current_phase + 1. This enforces sequential phase processing. Phase 0 handling is special as it's used for hashing and requires dropping all open sorts. The tuplesort management is complex: existing input sorts are always cleaned up, output sorts become input sorts for the next phase (with performsort() called), and new output sorts are created for non-final phases.