# row_is_in_frame

## Location
src/backend/executor/nodeWindowAgg.c: 1385 - 1484

## Overview
This static function determines whether a specific row is included in the current rows window frame according to the configured framing rules.

## Definition
```c
static int row_is_in_frame(WindowAggState *winstate, int64 pos, TupleTableSlot *slot)
```

## Detailed Description
The `row_is_in_frame` function implements the core logic for determining window frame membership. It evaluates a row at a given position against the current window frame definition, considering frame start/end boundaries, frame types (ROWS, RANGE, GROUPS), and exclusion clauses.

The function returns one of three values: -1 if the row is out of frame and no succeeding rows can be in frame, 0 if the row is out of frame but succeeding rows might be in frame, or 1 if the row is in frame. This tri-state return value helps optimize frame evaluation by indicating when frame scanning can be terminated early.

The function handles different frame boundary types: CURRENT ROW boundaries (with peer checking for RANGE/GROUPS), OFFSET boundaries (with different calculation methods for ROWS vs RANGE/GROUPS), and various exclusion clauses (EXCLUDE CURRENT ROW, EXCLUDE GROUP, EXCLUDE TIES).

## Parameters / Member Variables
- `winstate`: The WindowAggState containing frame configuration, position tracking, and execution state
- `pos`: The position of the row being tested for frame membership (must be >= 0)
- `slot`: The TupleTableSlot containing the row data for peer comparison and evaluation

## Dependencies
- Functions called/Symbols referenced:
  - update_frameheadpos
  - update_frametailpos
  - update_grouptailpos
  - are_peers
  - DatumGetInt64
- Called from (representative examples):
  - eval_windowaggregates
  - WinGetFuncArgInFrame

## Notes and Other Information
- The caller must ensure the row is already in the partition before calling this function
- May clobber winstate->temp_slot_2 during evaluation
- Optimizes performance by avoiding unnecessary calls to update_frametailpos in simple ROWS cases
- Handles peer determination for RANGE and GROUPS frame types using the are_peers function
- Supports all standard SQL window frame options including PRECEDING/FOLLOWING offsets
- For exclusion clauses, treats all rows as peers when there is no ORDER BY clause (ordNumCols == 0)
- The function encapsulates all framing rules in one place for consistency across window function evaluation