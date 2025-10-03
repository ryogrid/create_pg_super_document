# ExecSort

## Location
[src/backend/executor/nodeSort.c:50-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSort.c#L50-L220)

## Overview
Executes tuple sorting operations, either sorting all tuples from the outer subtree using tuplesort and returning them one by one, with optimization for single-column datum sorts versus multi-column tuple sorts.

## Definition

```c
structure
	 */
	sortstate = makeNode(SortState);
```
## Detailed Description
ExecSort is the main execution function for Sort plan nodes in PostgreSQL's executor. It operates in two phases:

1. **Initial sorting phase** (when  is false): Reads all tuples from the outer subtree, feeds them to the tuplesort module, and performs the complete sort operation. The function optimizes performance by choosing between two sorting strategies:
   - **Datum sort**: When sorting a single column, which is significantly faster for pass-by-value types
   - **Tuple sort**: When sorting multiple columns or complex data types

2. **Tuple retrieval phase**: On subsequent calls, returns pre-sorted tuples one by one from the tuplesort state.

The function supports various tuplesort options including random access, bounded sorts, and parallel worker statistics collection. It handles both forward and backward scan directions and manages memory efficiently through the work_mem parameter.

## Parameters / Member Variables
- : The PlanState structure containing the SortState node and execution context

## Dependencies
- Functions called/Symbols referenced:
  - : Cast pstate to SortState
  - : Check for query cancellation
  - : Get the outer plan state
  - : Get result tuple descriptor from outer node
  - : Initialize datum-based tuplesort
  - : Initialize tuple-based tuplesort  
  - : Set bound for bounded sorts
  - : Execute outer plan node to get tuples
  - /: Feed data to tuplesort
  - : Complete the sorting operation
  - /: Retrieve sorted tuples
  - /: Manage result tuple slots
- Called from (representative examples):
  - : During sort node initialization

## Notes and Other Information
- The function uses conditional compilation with SO1_printf for debugging output
- Supports parallel execution with worker statistics collection via shared_info
- Optimizes memory usage by choosing appropriate sorting strategy based on data characteristics  
- Handles scan direction changes by temporarily forcing ForwardScanDirection during initial sort phase
- Manages bounded sorts through tuplesort_set_bound for memory efficiency in TOP-N queries

## Simplified Source

```c
static TupleTableSlot *
ExecSort(PlanState *pstate)
{
    SortState *node = castNode(SortState, pstate);
    EState *estate = node->ss.ps.state;
    ScanDirection dir = estate->es_direction;
    Tuplesortstate *tuplesortstate = (Tuplesortstate *) node->tuplesortstate;
    TupleTableSlot *slot;

    CHECK_FOR_INTERRUPTS();

    // Phase 1: Initial sorting (read all tuples and sort them)
    if (!node->sort_Done) {
        Sort *plannode = (Sort *) node->ss.ps.plan;
        PlanState *outerNode = outerPlanState(node);
        TupleDesc tupDesc = ExecGetResultType(outerNode);
        int tuplesortopts = TUPLESORT_NONE;

        // Force forward scan during sorting
        estate->es_direction = ForwardScanDirection;

        // Set sort options
        if (node->randomAccess)
            tuplesortopts |= TUPLESORT_RANDOMACCESS;
        if (node->bounded)
            tuplesortopts |= TUPLESORT_ALLOWBOUNDED;

        // Initialize appropriate sort type
        if (node->datumSort) {
            // Single column optimization - sort datums only
            tuplesortstate = tuplesort_begin_datum(
                TupleDescAttr(tupDesc, 0)->atttypid,
                plannode->sortOperators[0],
                plannode->collations[0],
                plannode->nullsFirst[0],
                work_mem, NULL, tuplesortopts);
        } else {
            // Multi-column sort - sort complete tuples
            tuplesortstate = tuplesort_begin_heap(
                tupDesc, plannode->numCols,
                plannode->sortColIdx, plannode->sortOperators,
                plannode->collations, plannode->nullsFirst,
                work_mem, NULL, tuplesortopts);
        }

        // Set bound for TOP-N queries
        if (node->bounded)
            tuplesort_set_bound(tuplesortstate, node->bound);
        node->tuplesortstate = (void *) tuplesortstate;

        // Feed all input tuples to sort
        if (node->datumSort) {
            // Datum sort: extract first column value
            for (;;) {
                slot = ExecProcNode(outerNode);
                if (TupIsNull(slot))
                    break;
                slot_getsomeattrs(slot, 1);
                tuplesort_putdatum(tuplesortstate,
                                 slot->tts_values[0],
                                 slot->tts_isnull[0]);
            }
        } else {
            // Tuple sort: feed complete tuples
            for (;;) {
                slot = ExecProcNode(outerNode);
                if (TupIsNull(slot))
                    break;
                tuplesort_puttupleslot(tuplesortstate, slot);
            }
        }

        // Complete the sort
        tuplesort_performsort(tuplesortstate);

        // Restore original scan direction and mark as done
        estate->es_direction = dir;
        node->sort_Done = true;
        node->bounded_Done = node->bounded;
        node->bound_Done = node->bound;

        // Collect parallel worker statistics if needed
        if (node->shared_info && node->am_worker) {
            TuplesortInstrumentation *si =
                &node->shared_info->sinstrument[ParallelWorkerNumber];
            tuplesort_get_stats(tuplesortstate, si);
        }
    }

    // Phase 2: Return next sorted tuple
    slot = node->ss.ps.ps_ResultTupleSlot;

    if (node->datumSort) {
        // Handle datum sort results
        ExecClearTuple(slot);
        if (tuplesort_getdatum(tuplesortstate, ScanDirectionIsForward(dir),
                              false, &(slot->tts_values[0]),
                              &(slot->tts_isnull[0]), NULL))
            ExecStoreVirtualTuple(slot);
    } else {
        // Handle tuple sort results
        tuplesort_gettupleslot(tuplesortstate, ScanDirectionIsForward(dir),
                              false, slot, NULL);
    }

    return slot;
}
```