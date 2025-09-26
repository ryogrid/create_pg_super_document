# ExecIncrementalSort

## Location
src/backend/executor/nodeIncrementalSort.c: 495 - 975

## Overview
The main execution function for the incremental sort executor node that efficiently sorts data by leveraging pre-sorted input on a prefix of the target sort columns.

## Definition

```c
structures.
		 */
		if (fullsort_state == NULL)
		{
			/*
			 * Initialize presorted column support structures for
			 * isCurrentGroup(). It's correct to do this along with the
			 * initial initialization for the full sort state (and not for the
			 * prefix sort state) since we always load the full sort state
			 * first.
			 */
			preparePresortedCols(node);

			/*
			 * Since we optimize small prefix key groups by accumulating a
			 * minimum number of tuples before sorting, we can't assume that a
			 * group of tuples all have the same prefix key values. Hence we
			 * setup the full sort tuplesort to sort by all requested sort
			 * keys.
			 */
			fullsort_state = tuplesort_begin_heap(tupDesc,
												  plannode->sort.numCols,
												  plannode->sort.sortColIdx,
												  plannode->sort.sortOperators,
												  plannode->sort.collations,
												  plannode->sort.nullsFirst,
												  work_mem,
												  NULL,
												  node->bounded ?
												  TUPLESORT_ALLOWBOUNDED :
												  TUPLESORT_NONE);
			node->fullsort_state = fullsort_state;
		}
		else
		{
			/* Reset sort for the next batch. */
			tuplesort_reset(fullsort_state);
		}

		/*
		 * Calculate the remaining tuples left if bounded and configure both
		 * bounded sort and the minimum group size accordingly.
		 */
		if (node->bounded)
		{
			int64		currentBound = node->bound - node->bound_Done;

			/*
			 * Bounded sort isn't likely to be a useful optimization for full
			 * sort mode since we limit full sort mode to a relatively small
			 * number of tuples and tuplesort doesn't switch over to top-n
			 * heap sort anyway unless it hits (2 * bound) tuples.
			 */
			if (currentBound < DEFAULT_MIN_GROUP_SIZE)
				tuplesort_set_bound(fullsort_state, currentBound);

			minGroupSize = Min(DEFAULT_MIN_GROUP_SIZE, currentBound);
		}
		else
			minGroupSize = DEFAULT_MIN_GROUP_SIZE;
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
- : Pointer to PlanState (cast to IncrementalSortState) containing the execution state and configuration for the incremental sort operation

## Dependencies
- Functions called/Symbols referenced:
  - castNode (safely cast plan state)
  - CHECK_FOR_INTERRUPTS (check for query cancellation)
  - tuplesort_gettupleslot (retrieve tuple from sort state)
  - tuplesort_puttupleslot (add tuple to sort state) 
  - tuplesort_performsort (execute the sort operation)
  - tuplesort_begin_heap (initialize new sort state)
  - tuplesort_reset (reset sort state for reuse)
  - tuplesort_set_bound (set bound for bounded sorts)
  - tuplesort_used_bound (check if bounded sort was used)
  - ExecProcNode (get next tuple from outer node)
  - preparePresortedCols (initialize comparison functions)
  - isCurrentGroup (check if tuple belongs to current group)
  - switchToPresortedPrefixMode (transition to prefix sort mode)
  - ExecGetResultType (get tuple descriptor)
  - ExecCopySlot/ExecClearTuple (tuple slot management)
  - outerPlanState (get outer plan state)
  - INSTRUMENT_SORT_GROUP (macro for performance instrumentation)
  - Various constants: INCSORT_* execution states, DEFAULT_MIN_GROUP_SIZE, DEFAULT_MAX_FULL_SORT_GROUP_SIZE
- Called from (representative examples):
  - ExecInitIncrementalSort (registered as the execution function)

## Notes and Other Information
- This is the primary entry point for incremental sort execution in PostgreSQL's executor
- Implements sophisticated state machine logic to optimize between different sorting strategies
- Handles both forward and backward scan directions by temporarily forcing forward direction during accumulation
- Supports bounded sorts with dynamic bound adjustment as tuples are processed
- Provides extensive debugging output through SO_printf macros for troubleshooting and performance analysis
- Critical for query performance when dealing with large datasets that are partially pre-sorted
- The function maintains complex state across calls to handle streaming execution model
- Memory usage is optimized by reusing sort states and minimizing tuple copying