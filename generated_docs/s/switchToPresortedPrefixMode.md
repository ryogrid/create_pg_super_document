# switchToPresortedPrefixMode

## Location
[src/backend/executor/nodeIncrementalSort.c:286-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIncrementalSort.c#L286-L466)

## Overview
A static function that optimizes tuple sorting by switching from full-column sorting to prefix-optimized sorting when a large batch of tuples with identical pre-sorted prefix values is detected.

## Definition

```c
static void
switchToPresortedPrefixMode(PlanState *pstate)
```
## Detailed Description
This function implements a key optimization in the incremental sort algorithm. When the executor determines that it has encountered a large batch of tuples all having the same pre-sorted prefix values, it switches to an optimized sorting mode that only sorts on the remaining (unsorted) suffix keys, rather than sorting on all columns.

The function handles the complex transition between sorting modes by:
1. Configuring a new prefix sort state that only sorts on suffix columns
2. Transferring tuples from the full sort state to the prefix sort state
3. Verifying that transferred tuples belong to the same prefix group using isCurrentGroup
4. Handling group boundaries when multiple prefix groups exist
5. Setting appropriate bounds for bounded sorts
6. Managing execution state transitions

The optimization is based on the assumption that if we've seen many tuples with the same prefix values, we're likely to see many more, making prefix-optimized sorting more efficient.

## Parameters / Member Variables
- : Pointer to PlanState (cast to IncrementalSortState) containing the incremental sort execution state

## Dependencies
- Functions called/Symbols referenced:
  - castNode (safely cast plan state to IncrementalSortState)
  - outerPlanState (get outer plan state)
  - ExecGetResultType (get tuple descriptor from outer node)
  - tuplesort_begin_heap (create new tuplesort state for prefix sorting)
  - tuplesort_reset (reset existing tuplesort state)
  - tuplesort_set_bound (set bound for bounded sorts)
  - tuplesort_gettupleslot (get tuple from full sort state)
  - tuplesort_puttupleslot (put tuple into prefix sort state)
  - tuplesort_performsort (perform the prefix sort)
  - isCurrentGroup (check if tuple belongs to current group)
  - ExecCopySlot (copy tuple between slots)
  - ExecClearTuple (clear tuple slot)
  - INSTRUMENT_SORT_GROUP (macro for instrumentation)
  - Various constants: INCSORT_LOADPREFIXSORT, INCSORT_READPREFIXSORT, TUPLESORT_ALLOWBOUNDED, TUPLESORT_NONE
- Called from (representative examples):
  - ExecIncrementalSort (main execution function, multiple decision points)

## Notes and Other Information
- This function is called when the algorithm detects a potentially large prefix group
- Handles both first-time prefix sort initialization and reuse of existing prefix sort state
- Supports bounded sorts by carrying forward bound information and adjusting for already processed tuples
- Manages complex state transitions between different execution phases
- Uses debugging output macros (SO_printf, SO1_printf, SO2_printf) for tracing execution
- Critical for achieving good performance on inputs with many tuples having identical prefix values
- The function can handle cases where not all accumulated tuples belong to the same prefix group