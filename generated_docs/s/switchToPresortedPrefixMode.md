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
  - [ExecGetResultType](../E/ExecGetResultType.md) (get tuple descriptor from outer node)
  - [tuplesort_begin_heap](../t/tuplesort_begin_heap.md) (create new tuplesort state for prefix sorting)
  - [tuplesort_reset](../t/tuplesort_reset.md) (reset existing tuplesort state)
  - [tuplesort_set_bound](../t/tuplesort_set_bound.md) (set bound for bounded sorts)
  - [tuplesort_gettupleslot](../t/tuplesort_gettupleslot.md) (get tuple from full sort state)
  - [tuplesort_puttupleslot](../t/tuplesort_puttupleslot.md) (put tuple into prefix sort state)
  - [tuplesort_performsort](../t/tuplesort_performsort.md) (perform the prefix sort)
  - [isCurrentGroup](../i/isCurrentGroup.md) (check if tuple belongs to current group)
  - [ExecCopySlot](../E/ExecCopySlot.md) (copy tuple between slots)
  - [ExecClearTuple](../E/ExecClearTuple.md) (clear tuple slot)
  - INSTRUMENT_SORT_GROUP (macro for instrumentation)
  - Various constants: INCSORT_LOADPREFIXSORT, INCSORT_READPREFIXSORT, TUPLESORT_ALLOWBOUNDED, TUPLESORT_NONE
- Called from (representative examples):
  - [ExecIncrementalSort](../E/ExecIncrementalSort.md) (main execution function, multiple decision points)

## Notes and Other Information
- This function is called when the algorithm detects a potentially large prefix group
- Handles both first-time prefix sort initialization and reuse of existing prefix sort state
- Supports bounded sorts by carrying forward bound information and adjusting for already processed tuples
- Manages complex state transitions between different execution phases
- Uses debugging output macros (SO_printf, SO1_printf, SO2_printf) for tracing execution
- Critical for achieving good performance on inputs with many tuples having identical prefix values
- The function can handle cases where not all accumulated tuples belong to the same prefix group

## Simplified Source

```c
static void switchToPresortedPrefixMode(PlanState *pstate)
{
    IncrementalSortState *node = castNode(IncrementalSortState, pstate);
    IncrementalSort *plannode = castNode(IncrementalSort, node->ss.ps.plan);
    PlanState *outerNode = outerPlanState(node);
    TupleDesc tupDesc = ExecGetResultType(outerNode);

    // Step 1: Configure prefix sort state (first time) or reset for new group
    if (node->prefixsort_state == NULL)
    {
        // Create new tuplesort that only sorts on suffix columns
        int nPresortedCols = plannode->nPresortedCols;
        int suffixCols = plannode->sort.numCols - nPresortedCols;

        node->prefixsort_state = tuplesort_begin_heap(
            tupDesc,
            suffixCols,                                           // Only sort remaining columns
            &(plannode->sort.sortColIdx[nPresortedCols]),        // Start from suffix columns
            &(plannode->sort.sortOperators[nPresortedCols]),
            &(plannode->sort.collations[nPresortedCols]),
            &(plannode->sort.nullsFirst[nPresortedCols]),
            work_mem,
            NULL,
            node->bounded ? TUPLESORT_ALLOWBOUNDED : TUPLESORT_NONE);
    }
    else
    {
        // Reuse existing prefix sort state for next group
        tuplesort_reset(node->prefixsort_state);
    }

    // Step 2: Set bound for bounded sorts
    if (node->bounded)
    {
        tuplesort_set_bound(node->prefixsort_state, node->bound - node->bound_Done);
    }

    // Step 3: Transfer tuples from full sort to prefix sort
    int64 nTuples = 0;

    for (nTuples = 0; nTuples < node->n_fullsort_remaining; nTuples++)
    {
        // Handle carried-over tuple from previous group
        if (nTuples == 0 && !TupIsNull(node->transfer_tuple))
        {
            tuplesort_puttupleslot(node->prefixsort_state, node->transfer_tuple);
            ExecCopySlot(node->group_pivot, node->transfer_tuple);
        }
        else
        {
            // Get next tuple from full sort
            tuplesort_gettupleslot(node->fullsort_state,
                                 ScanDirectionIsForward(node->ss.ps.state->es_direction),
                                 false, node->transfer_tuple, NULL);

            // Set group pivot on first iteration
            if (TupIsNull(node->group_pivot))
                ExecCopySlot(node->group_pivot, node->transfer_tuple);

            // Check if tuple belongs to current prefix group
            if (isCurrentGroup(node, node->group_pivot, node->transfer_tuple))
            {
                tuplesort_puttupleslot(node->prefixsort_state, node->transfer_tuple);
            }
            else
            {
                // Different prefix group - stop transferring and carry over this tuple
                ExecClearTuple(node->group_pivot);
                break;
            }
        }
    }

    // Step 4: Update remaining tuple count
    node->n_fullsort_remaining -= nTuples;

    // Step 5: Determine next execution state
    if (node->n_fullsort_remaining == 0)
    {
        // All tuples transferred - continue loading more from input
        ExecCopySlot(node->group_pivot, node->transfer_tuple);
        node->execution_status = INCSORT_LOADPREFIXSORT;
        ExecClearTuple(node->transfer_tuple);
    }
    else
    {
        // Some tuples remain - sort current batch first
        tuplesort_performsort(node->prefixsort_state);

        if (node->bounded)
            node->bound_Done = Min(node->bound, node->bound_Done + nTuples);

        node->execution_status = INCSORT_READPREFIXSORT;
    }
}
```