# update_frametailpos

## Location
src/backend/executor/nodeWindowAgg.c: 1735 - 1984

## Overview
A static function that computes and sets the frame tail position for the current row in window function processing, determining where the window frame ends based on various boundary modes including UNBOUNDED FOLLOWING, CURRENT ROW, and offset-based frames.

## Definition


## Detailed Description
This function is the counterpart to update_frameheadpos and is responsible for determining where the window frame ends relative to the current row being processed in window aggregation operations. The frame tail position represents the first row *after* the last row that should be included in the frame.

The function handles all supported frame end boundary types:
- **UNBOUNDED FOLLOWING**: Frame extends to the end of the partition
- **CURRENT ROW**: Frame ends at current row or last peer depending on frame mode  
- **OFFSET-based frames**: Frame ends at a specific offset from current row

Similar to frame head processing, the behavior varies by frame mode:
- **ROWS mode**: Operates on physical row positions with direct offset calculations
- **RANGE mode**: Uses ORDER BY column values and in_range functions to determine logical boundaries
- **GROUPS mode**: Works with peer groups where rows having identical ORDER BY values are treated as a single unit

The function maintains frame tail tracking through  and , ensures proper tuple spooling when accessing future rows, and uses memory context management for efficiency.

## Parameters / Member Variables
- : WindowAggState pointer containing all window function execution state including:
  - Frame options and end boundary specifications
  - Current row position and partition context
  - Tuple store for partition buffering and navigation
  - Frame tail position cache and validation flags
  - Memory contexts and temporary tuple slots

## Dependencies
- Functions called/Symbols referenced:
  - [spool_tuples](../s/spool_tuples.md) (tuple buffering to ensure data availability)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory management)
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md) (tuple store navigation)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md) (tuple retrieval from buffer)
  - are_peers (peer comparison for RANGE/GROUPS modes)
  - slot_getattr (tuple attribute extraction for RANGE mode)
  - [FunctionCall5Coll](../F/FunctionCall5Coll.md) (in_range function calls for RANGE mode)
  - ExecCopySlot/ExecClearTuple (tuple slot management in GROUPS mode)
  - [DatumGetInt64](../D/DatumGetInt64.md) (offset value extraction)
- Called from (representative examples):
  - [row_is_in_frame](../r/row_is_in_frame.md) (for frame membership testing)
  - [ExecWindowAgg](../E/ExecWindowAgg.md) (main window aggregation execution)
  - [WinGetFuncArgInFrame](../W/WinGetFuncArgInFrame.md) (window function argument retrieval)

## Notes and Other Information
- The function may clobber  during GROUPS mode processing
- Frame tail position computation ignores window exclusion clauses - exclusion is applied separately
- Uses memory context switching to prevent accumulation of temporary allocations
- Maintains frame tail validity flags to avoid redundant computation for the same row
- For RANGE mode, requires exactly one ORDER BY column and uses specialized in_range functions
- Handles NULL values in ORDER BY columns according to NULLS FIRST/LAST specifications
- Frame tail position is always one past the last included row (exclusive end boundary)
- Critical for performance as it determines the ending boundary for all frame-based window function computations