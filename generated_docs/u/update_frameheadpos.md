# update_frameheadpos

## Location
[src/backend/executor/nodeWindowAgg.c:1485-1734](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L1485-L1734)

## Overview
A static function that computes and sets the frame head position for the current row in window function processing, handling various frame boundary modes including UNBOUNDED PRECEDING, CURRENT ROW, and offset-based frames.

## Definition

```c
static void
update_frameheadpos(WindowAggState *winstate)
```
## Detailed Description
This function is responsible for determining where the window frame begins relative to the current row being processed in a window aggregation operation. It handles all supported frame start boundary types defined by SQL window functions:

- **UNBOUNDED PRECEDING**: Frame always starts at row 0 (first row of partition)
- **CURRENT ROW**: Frame starts at current row or first peer depending on frame mode
- **OFFSET-based frames**: Frame starts at a specific offset from current row

The function operates differently based on the frame mode:
- **ROWS mode**: Treats each row individually, computing physical row positions
- **RANGE mode**: Groups logically equivalent rows using ORDER BY column values and in_range functions
- **GROUPS mode**: Works with peer groups, where rows with identical ORDER BY values form a group

The function uses tuple stores to buffer partition data and maintains frame head position tracking through  and . It employs memory context switching for proper memory management and may trigger tuple spooling when accessing rows beyond the current buffer.

## Parameters / Member Variables
- : WindowAggState pointer containing all window function execution state including:

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory management)
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md) (tuple store navigation)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md) (tuple retrieval)
  - are_peers (peer comparison for RANGE/GROUPS modes)
  - [spool_tuples](../s/spool_tuples.md) (tuple buffering)
  - slot_getattr (tuple attribute extraction)
  - [FunctionCall5Coll](../F/FunctionCall5Coll.md) (in_range function calls for RANGE mode)
  - ExecCopySlot/ExecClearTuple (tuple slot management)
  - [DatumGetInt64](../D/DatumGetInt64.md) (offset value extraction)
- Called from (representative examples):
  - [eval_windowaggregates](../e/eval_windowaggregates.md) (during aggregate window function evaluation)
  - [row_is_in_frame](../r/row_is_in_frame.md) (for frame membership testing)
  - [ExecWindowAgg](../E/ExecWindowAgg.md) (main window aggregation execution)
  - [WinGetFuncArgInFrame](../W/WinGetFuncArgInFrame.md) (window function argument retrieval)

## Notes and Other Information
- The function may clobber  during GROUPS mode processing
- Frame head position computation ignores window exclusion clauses - exclusion is applied later
- Uses short-lived memory context switching to prevent memory leaks in repeated calls
- Maintains frame head validity flags to avoid redundant computation for the same row
- For RANGE mode with single ORDER BY column, uses specialized in_range functions for boundary checking
- Handles NULL values in ORDER BY columns according to NULLS FIRST/LAST specifications
- Critical for window function performance as it determines the starting boundary for all frame-based computations