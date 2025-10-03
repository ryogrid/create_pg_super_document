# ExecIncrementalSort

## Location
[src/backend/executor/nodeIncrementalSort.c:495-975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIncrementalSort.c#L495-L975)

## Overview
The main execution function for the incremental sort executor node that efficiently sorts data by leveraging pre-sorted input on a prefix of the target sort columns.

## Definition

```c
static TupleTableSlot *
ExecIncrementalSort(PlanState *pstate)
```
## Detailed Description
ExecIncrementalSort is the core execution function that implements PostgreSQL's incremental sort algorithm. This function operates on the assumption that the input from the outer subtree is already sorted by some prefix of the target sort columns, allowing it to sort tuples in smaller, more efficient batches rather than sorting all data at once.

The algorithm works through several execution states:

1. **INCSORT_LOADFULLSORT**: Accumulates tuples until reaching a minimum group size or detecting a prefix group boundary, then sorts using all columns
2. **INCSORT_READFULLSORT**: Returns sorted tuples from the full sort state
3. **INCSORT_LOADPREFIXSORT**: When a large prefix group is detected, loads tuples that share the same prefix values
4. **INCSORT_READPREFIXSORT**: Returns tuples from the prefix-optimized sort state

The function dynamically switches between full sorting (for small groups) and prefix-optimized sorting (for large groups with identical prefix values) to optimize performance. It handles bounded sorts, manages memory efficiently, and provides comprehensive instrumentation for performance analysis.

## Parameters / Member Variables
- `pstate`: Pointer to PlanState (cast to IncrementalSortState) containing the execution state and configuration for the incremental sort operation

## Dependencies
- Functions called/Symbols referenced:
  - castNode (safely cast plan state)
  - CHECK_FOR_INTERRUPTS (check for query cancellation)
  - [tuplesort_gettupleslot](../t/tuplesort_gettupleslot.md) (retrieve tuple from sort state)
  - [tuplesort_puttupleslot](../t/tuplesort_puttupleslot.md) (add tuple to sort state) 
  - [tuplesort_performsort](../t/tuplesort_performsort.md) (execute the sort operation)
  - [tuplesort_begin_heap](../t/tuplesort_begin_heap.md) (initialize new sort state)
  - [tuplesort_reset](../t/tuplesort_reset.md) (reset sort state for reuse)
  - [tuplesort_set_bound](../t/tuplesort_set_bound.md) (set bound for bounded sorts)
  - [tuplesort_used_bound](../t/tuplesort_used_bound.md) (check if bounded sort was used)
  - [ExecProcNode](ExecProcNode.md) (get next tuple from outer node)
  - [preparePresortedCols](../p/preparePresortedCols.md) (initialize comparison functions)
  - [isCurrentGroup](../i/isCurrentGroup.md) (check if tuple belongs to current group)
  - [switchToPresortedPrefixMode](../s/switchToPresortedPrefixMode.md) (transition to prefix sort mode)
  - [ExecGetResultType](ExecGetResultType.md) (get tuple descriptor)
  - [ExecCopySlot](ExecCopySlot.md)/ExecClearTuple (tuple slot management)
  - outerPlanState (get outer plan state)
  - INSTRUMENT_SORT_GROUP (macro for performance instrumentation)
  - Various constants: INCSORT_* execution states, DEFAULT_MIN_GROUP_SIZE, DEFAULT_MAX_FULL_SORT_GROUP_SIZE
- Called from (representative examples):
  - [ExecInitIncrementalSort](ExecInitIncrementalSort.md) (registered as the execution function)

## Notes and Other Information
- This is the primary entry point for incremental sort execution in PostgreSQL's executor
- Implements sophisticated state machine logic to optimize between different sorting strategies
- Handles both forward and backward scan directions by temporarily forcing forward direction during accumulation
- Supports bounded sorts with dynamic bound adjustment as tuples are processed
- Provides extensive debugging output through SO_printf macros for troubleshooting and performance analysis
- Critical for query performance when dealing with large datasets that are partially pre-sorted
- The function maintains complex state across calls to handle streaming execution model
- Memory usage is optimized by reusing sort states and minimizing tuple copying

## Simplified Source

```c
// Simplified version of ExecIncrementalSort
static TupleTableSlot *
ExecIncrementalSort(PlanState *pstate)
{
    IncrementalSortState *node = castNode(IncrementalSortState, pstate);
    EState *estate = node->ss.ps.state;
    TupleTableSlot *slot;
    PlanState *outerNode = outerPlanState(node);

    // Return sorted tuples if we have them ready
    if (node->execution_status == INCSORT_READFULLSORT ||
        node->execution_status == INCSORT_READPREFIXSORT)
    {
        TuplesortState *read_sortstate = (node->execution_status == INCSORT_READFULLSORT) ?
            node->fullsort_state : node->prefixsort_state;

        slot = node->ss.ps.ps_ResultTupleSlot;
        if (tuplesort_gettupleslot(read_sortstate, true, false, slot, NULL) ||
            node->outerNodeDone)
            return slot;

        // Need to load next group
        node->execution_status = INCSORT_LOADFULLSORT;
    }

    // Load and sort full groups (main algorithm)
    if (node->execution_status == INCSORT_LOADFULLSORT)
    {
        // Initialize sort state if needed
        if (node->fullsort_state == NULL)
        {
            preparePresortedCols(node);
            node->fullsort_state = tuplesort_begin_heap(/* sort parameters */);
        } else {
            tuplesort_reset(node->fullsort_state);
        }

        int64 nTuples = 0;
        int64 minGroupSize = DEFAULT_MIN_GROUP_SIZE;

        // Add carry-over tuple if any
        if (!TupIsNull(node->group_pivot))
        {
            tuplesort_puttupleslot(node->fullsort_state, node->group_pivot);
            nTuples++;
        }

        // Collect tuples until group boundary or minimum size
        for (;;)
        {
            slot = ExecProcNode(outerNode);
            if (TupIsNull(slot))
            {
                node->outerNodeDone = true;
                break;
            }

            // Check if we've reached minimum group size and found new group
            if (nTuples >= minGroupSize &&
                !isCurrentGroup(node, node->group_pivot, slot))
            {
                // Save tuple for next group and stop collecting
                ExecCopySlot(node->group_pivot, slot);
                break;
            }

            // Add tuple to current group
            tuplesort_puttupleslot(node->fullsort_state, slot);
            nTuples++;

            // Set pivot when we reach minimum size
            if (nTuples == minGroupSize)
                ExecCopySlot(node->group_pivot, slot);

            // Switch to prefix mode for very large groups
            if (nTuples > DEFAULT_MAX_FULL_SORT_GROUP_SIZE)
            {
                tuplesort_performsort(node->fullsort_state);
                node->n_fullsort_remaining = nTuples;
                switchToPresortedPrefixMode(pstate);
                break;
            }
        }

        // Sort the collected group
        if (node->execution_status == INCSORT_LOADFULLSORT)
        {
            tuplesort_performsort(node->fullsort_state);
            node->execution_status = INCSORT_READFULLSORT;
        }
    }

    // Handle prefix sort mode loading
    if (node->execution_status == INCSORT_LOADPREFIXSORT)
    {
        // Load tuples with matching prefix until boundary found
        for (;;)
        {
            slot = ExecProcNode(outerNode);
            if (TupIsNull(slot) ||
                !isCurrentGroup(node, node->group_pivot, slot))
            {
                if (!TupIsNull(slot))
                    ExecCopySlot(node->group_pivot, slot);
                else
                    node->outerNodeDone = true;
                break;
            }
            tuplesort_puttupleslot(node->prefixsort_state, slot);
        }

        tuplesort_performsort(node->prefixsort_state);
        node->execution_status = INCSORT_READPREFIXSORT;
    }

    // Return first tuple from newly sorted group
    TuplesortState *read_sortstate = (node->execution_status == INCSORT_READFULLSORT) ?
        node->fullsort_state : node->prefixsort_state;
    slot = node->ss.ps.ps_ResultTupleSlot;
    tuplesort_gettupleslot(read_sortstate, true, false, slot, NULL);
    return slot;
}
```