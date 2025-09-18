# ExecWindowAgg

## Location
src/backend/executor/nodeWindowAgg.c: 2046 - 2373

## Overview
The main execution function for window aggregation nodes, processing tuples from the outer subplan through a tuplestore and evaluating window functions to produce exactly the same number of output rows as the input.

## Definition


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
  - Window function specifications and frame options
  - Tuple store for partition buffering
  - Current position and peer group tracking
  - Frame boundary positions and validation flags
  - Expression contexts for evaluation
  - Status flags for execution mode management

## Dependencies
- Functions called/Symbols referenced:
  - castNode (safe casting to WindowAggState)
  - CHECK_FOR_INTERRUPTS (query cancellation handling)
  - ExecEvalExprSwitchContext (frame offset expression evaluation)
  - [get_typlenbyval](../g/get_typlenbyval.md)/datumCopy (offset value copying)
  - [begin_partition](../b/begin_partition.md)/release_partition (partition lifecycle management)
  - [spool_tuples](../s/spool_tuples.md) (tuple buffering and data availability)
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md)/tuplestore_gettupleslot (tuple access)
  - are_peers (peer group detection)
  - [eval_windowfunction](../e/eval_windowfunction.md)/eval_windowaggregates (window function execution)
  - [update_frameheadpos](../u/update_frameheadpos.md)/update_frametailpos/update_grouptailpos (frame boundary maintenance)
  - [tuplestore_trim](../t/tuplestore_trim.md) (memory management)
  - ExecProject (output tuple formation)
  - ExecQual (run condition and qualification evaluation)
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