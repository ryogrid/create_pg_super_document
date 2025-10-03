# row_is_in_frame

## Location
[src/backend/executor/nodeWindowAgg.c:1385-1484](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L1385-L1484)

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
  - [update_frameheadpos](../u/update_frameheadpos.md)
  - [update_frametailpos](../u/update_frametailpos.md)
  - [update_grouptailpos](../u/update_grouptailpos.md)
  - [are_peers](../a/are_peers.md)
  - [DatumGetInt64](../D/DatumGetInt64.md)
- Called from (representative examples):
  - [eval_windowaggregates](../e/eval_windowaggregates.md)
  - [WinGetFuncArgInFrame](../W/WinGetFuncArgInFrame.md)

## Notes and Other Information
- The caller must ensure the row is already in the partition before calling this function
- May clobber winstate->temp_slot_2 during evaluation
- Optimizes performance by avoiding unnecessary calls to update_frametailpos in simple ROWS cases
- Handles peer determination for RANGE and GROUPS frame types using the are_peers function
- Supports all standard SQL window frame options including PRECEDING/FOLLOWING offsets
- For exclusion clauses, treats all rows as peers when there is no ORDER BY clause (ordNumCols == 0)
- The function encapsulates all framing rules in one place for consistency across window function evaluation

## Simplified Source

```c
static int
row_is_in_frame(WindowAggState *winstate, int64 pos, TupleTableSlot *slot)
{
    int frameOptions = winstate->frameOptions;

    Assert(pos >= 0);

    // Check frame start boundary
    update_frameheadpos(winstate);
    if (pos < winstate->frameheadpos)
        return 0;

    // Check frame end boundary
    if (frameOptions & FRAMEOPTION_END_CURRENT_ROW)
    {
        if (frameOptions & FRAMEOPTION_ROWS)
        {
            // For ROWS: positions after current row are out of frame
            if (pos > winstate->currentpos)
                return -1;
        }
        else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS))
        {
            // For RANGE/GROUPS: non-peer rows after current are out of frame
            if (pos > winstate->currentpos &&
                !are_peers(winstate, slot, winstate->ss.ss_ScanTupleSlot))
                return -1;
        }
        else
            Assert(false);
    }
    else if (frameOptions & FRAMEOPTION_END_OFFSET)
    {
        if (frameOptions & FRAMEOPTION_ROWS)
        {
            // Handle ROWS with offset
            int64 offset = DatumGetInt64(winstate->endOffsetValue);
            if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING)
                offset = -offset;

            if (pos > winstate->currentpos + offset)
                return -1;
        }
        else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS))
        {
            // For RANGE/GROUPS with offset, delegate to update_frametailpos
            update_frametailpos(winstate);
            if (pos >= winstate->frametailpos)
                return -1;
        }
        else
            Assert(false);
    }

    // Check exclusion clauses
    if (frameOptions & FRAMEOPTION_EXCLUDE_CURRENT_ROW)
    {
        if (pos == winstate->currentpos)
            return 0;
    }
    else if ((frameOptions & FRAMEOPTION_EXCLUDE_GROUP) ||
             ((frameOptions & FRAMEOPTION_EXCLUDE_TIES) &&
              pos != winstate->currentpos))
    {
        WindowAgg *node = (WindowAgg *) winstate->ss.ps.plan;

        // If no ORDER BY, all rows are peers with each other
        if (node->ordNumCols == 0)
            return 0;

        // Check group boundaries for exclusion
        if (pos >= winstate->groupheadpos)
        {
            update_grouptailpos(winstate);
            if (pos < winstate->grouptailpos)
                return 0;
        }
    }

    // Row is in frame
    return 1;
}
```