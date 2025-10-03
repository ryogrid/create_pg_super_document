# ExecWindowAgg

## Location
[src/backend/executor/nodeWindowAgg.c:2046-2373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L2046-L2373)

## Overview
The main execution function for window aggregation nodes, processing tuples from the outer subplan through a tuplestore and evaluating window functions to produce exactly the same number of output rows as the input.

## Definition

```c
structure
	 */
	winstate = makeNode(WindowAggState);
```
## Detailed Description
ExecWindowAgg is the core execution engine for window function processing in PostgreSQL. It implements a sophisticated stateful processing model that handles multiple window functions simultaneously while maintaining proper frame boundaries and partition management.

The function operates through several key phases:
1. **Initialization**: On first call, evaluates frame offset expressions and caches their values for the entire scan
2. **Partition Management**: Detects partition boundaries and manages transitions between partitions using  and 
3. **Tuple Buffering**: Uses a tuplestore to buffer partition data via , enabling random access for frame-based operations
4. **Current Row Processing**: Advances through rows within partitions, maintaining current position and peer group tracking
5. **Window Function Evaluation**: Executes both plain window functions () and window aggregates ()
6. **Frame Boundary Management**: Maintains frame head/tail positions through , , and 
7. **Performance Optimization**: Implements pass-through modes and run condition evaluation for early termination
8. **Projection**: Forms output tuples by combining window function results with current row data

The function handles complex scenarios including:
- Multiple partitions with different ORDER BY values
- ROWS, RANGE, and GROUPS frame modes
- Peer group detection for GROUPS mode and exclusion clauses
- Memory management through context switching and tuple store trimming
- Pass-through optimization when run conditions fail

## Parameters / Member Variables
- : PlanState pointer that must be castable to WindowAggState, containing:

## Dependencies
- Functions called/Symbols referenced:
  - castNode (safe casting to WindowAggState)
  - CHECK_FOR_INTERRUPTS (query cancellation handling)
  - [ExecEvalExprSwitchContext](ExecEvalExprSwitchContext.md) (frame offset expression evaluation)
  - [get_typlenbyval](../g/get_typlenbyval.md)/datumCopy (offset value copying)
  - [begin_partition](../b/begin_partition.md)/release_partition (partition lifecycle management)
  - [spool_tuples](../s/spool_tuples.md) (tuple buffering and data availability)
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md)/tuplestore_gettupleslot (tuple access)
  - [are_peers](../a/are_peers.md) (peer group detection)
  - [eval_windowfunction](../e/eval_windowfunction.md)/eval_windowaggregates (window function execution)
  - [update_frameheadpos](../u/update_frameheadpos.md)/update_frametailpos/update_grouptailpos (frame boundary maintenance)
  - [tuplestore_trim](../t/tuplestore_trim.md) (memory management)
  - [ExecProject](ExecProject.md) (output tuple formation)
  - [ExecQual](ExecQual.md) (run condition and qualification evaluation)
  - ResetExprContext (per-tuple memory cleanup)
- Called from (representative examples):
  - [ExecInitWindowAgg](ExecInitWindowAgg.md) (node initialization sets this as execution function)

## Notes and Other Information
- Returns exactly the same number of rows as the input (no filtering at the window level)
- Implements sophisticated performance optimizations including pass-through modes when run conditions fail
- Handles multiple execution states: WINDOWAGG_RUN, WINDOWAGG_PASSTHROUGH, WINDOWAGG_PASSTHROUGH_STRICT, WINDOWAGG_DONE
- Frame offset expressions are evaluated only once and cached for the entire scan for performance
- Uses memory context switching for proper memory management during long-running operations
- Supports both top-level and nested WindowAgg operations with different behavior for run condition failures
- Critical path function that must efficiently handle millions of rows while maintaining frame boundary accuracy
- Implements lazy evaluation strategies - frame boundaries are computed only when needed by window functions

## Simplified Source

```c
static TupleTableSlot *
ExecWindowAgg(PlanState *pstate)
{
    WindowAggState *winstate = castNode(WindowAggState, pstate);
    TupleTableSlot *slot;
    ExprContext *econtext;

    // Check if we're done processing
    if (winstate->status == WINDOWAGG_DONE)
        return NULL;

    // First-time setup: evaluate frame offset expressions
    if (winstate->all_first)
    {
        // Evaluate and cache frame start/end offsets
        if (winstate->frameOptions & FRAMEOPTION_START_OFFSET)
            winstate->startOffsetValue = ExecEvalExprSwitchContext(winstate->startOffset, ...);
        if (winstate->frameOptions & FRAMEOPTION_END_OFFSET)
            winstate->endOffsetValue = ExecEvalExprSwitchContext(winstate->endOffset, ...);
        winstate->all_first = false;
    }

    // Main processing loop
    for (;;)
    {
        // Initialize or advance current position
        if (winstate->buffer == NULL)
            begin_partition(winstate);  // Start new partition
        else
        {
            winstate->currentpos++;     // Move to next row
            // Invalidate frame boundaries since position changed
            winstate->framehead_valid = false;
            winstate->frametail_valid = false;
        }

        // Buffer tuples up to current position
        spool_tuples(winstate, winstate->currentpos);

        // Check if we need to move to next partition
        if (winstate->partition_spooled &&
            winstate->currentpos >= winstate->spooled_rows)
        {
            release_partition(winstate);
            if (winstate->more_partitions)
                begin_partition(winstate);
            else
            {
                winstate->status = WINDOWAGG_DONE;
                return NULL;
            }
        }

        // Read current row from tuplestore
        tuplestore_select_read_pointer(winstate->buffer, winstate->current_ptr);
        tuplestore_gettupleslot(winstate->buffer, true, true, winstate->ss.ss_ScanTupleSlot);

        // Handle peer group detection for GROUPS mode
        if (winstate->frameOptions & (FRAMEOPTION_GROUPS | FRAMEOPTION_EXCLUDE_GROUP) &&
            winstate->currentpos > 0)
        {
            if (!are_peers(winstate, previous_tuple, current_tuple))
            {
                winstate->currentgroup++;
                winstate->groupheadpos = winstate->currentpos;
            }
        }

        // Evaluate window functions (when not in pass-through mode)
        if (winstate->status == WINDOWAGG_RUN)
        {
            // Evaluate window functions
            for (int i = 0; i < winstate->numfuncs; i++)
            {
                if (!winstate->perfunc[i].plain_agg)
                    eval_windowfunction(winstate, &winstate->perfunc[i], ...);
            }

            // Evaluate aggregate functions
            if (winstate->numaggs > 0)
                eval_windowaggregates(winstate);
        }

        // Update frame boundary pointers
        if (winstate->framehead_ptr >= 0) update_frameheadpos(winstate);
        if (winstate->frametail_ptr >= 0) update_frametailpos(winstate);
        if (winstate->grouptail_ptr >= 0) update_grouptailpos(winstate);

        // Clean up tuplestore
        tuplestore_trim(winstate->buffer);

        // Project output tuple
        econtext = winstate->ss.ps.ps_ExprContext;
        econtext->ecxt_outertuple = winstate->ss.ss_ScanTupleSlot;
        slot = ExecProject(winstate->ss.ps.ps_ProjInfo);

        // Check run condition and qualification
        if (winstate->status == WINDOWAGG_RUN)
        {
            if (!ExecQual(winstate->runcondition, econtext))
            {
                // Switch to pass-through mode or finish
                if (winstate->use_pass_through)
                    winstate->status = winstate->top_window ?
                        WINDOWAGG_PASSTHROUGH_STRICT : WINDOWAGG_PASSTHROUGH;
                else
                {
                    winstate->status = WINDOWAGG_DONE;
                    return NULL;
                }
            }

            if (ExecQual(winstate->ss.ps.qual, econtext))
                break;  // Tuple passes qualification
        }
        else if (!winstate->top_window)
            break;  // Return tuple in pass-through mode for nested windows
    }

    return slot;
}
```

This function implements window aggregation processing by:
1. **Setup**: Evaluating frame offset expressions on first call
2. **Positioning**: Managing current row position within partitions
3. **Buffering**: Spooling input tuples into a tuplestore for random access
4. **Partitioning**: Detecting and transitioning between partitions
5. **Function Evaluation**: Computing window functions and aggregates for each row
6. **Frame Management**: Maintaining frame head/tail boundaries for window functions
7. **Optimization**: Using pass-through modes when run conditions fail
8. **Output**: Projecting final tuples with window function results