# WinGetFuncArgInFrame

## Location
[src/backend/executor/nodeWindowAgg.c:3398-3592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L3398-L3592)

## Overview
Evaluates a window function's argument expression on a specified row within the window frame, with sophisticated handling of frame exclusion options and positioning relative to frame boundaries.

## Definition

```c
Datum
WinGetFuncArgInFrame(WindowObject winobj, int argno,
					 int relpos, int seektype, bool set_mark,
					 bool *isnull, bool *isout)
```
## Detailed Description
This function provides frame-aware row positioning and argument evaluation for window functions. Unlike WinGetFuncArgInPartition, it operates strictly within the defined window frame and handles complex exclusion clauses that can create non-consecutive in-frame rows within the partition.

The function operates by:
1. Validating the window object and extracting state information
2. Computing absolute position based on seek type (HEAD or TAIL of frame):
   - WINDOW_SEEK_HEAD: Position relative to frame start
   - WINDOW_SEEK_TAIL: Position relative to frame end
   - WINDOW_SEEK_CURRENT: Not supported (unclear semantics when current row might not be in frame)
3. Applying complex exclusion clause adjustments:
   - EXCLUDE_CURRENT_ROW: Skips the current row if it would be accessed
   - EXCLUDE_GROUP: Skips the entire peer group containing the current row
   - EXCLUDE_TIES: Skips peer rows but keeps the current row
4. Fetching the tuple and verifying it's actually within the frame
5. Evaluating the argument expression if the row is in-frame

The exclusion logic ensures that when counting frame positions, only in-frame rows are considered, which can result in non-obvious position calculations.

## Parameters / Member Variables
- `winobj`: Window object containing frame definition and state
- `argno`: Zero-based index of the argument expression to evaluate
- `relpos`: Signed offset from the seek position (must be >= 0 for HEAD, <= 0 for TAIL)
- `seektype`: Position reference point (WINDOW_SEEK_HEAD or WINDOW_SEEK_TAIL only)
- `set_mark`: Whether to move the mark position (with exclusion-aware adjustments)
- `*isnull`: Output parameter receiving null status of the evaluated expression
- `*isout`: Output parameter indicating if the target position is outside the frame
## Dependencies
- Functions called/Symbols referenced:
  - WindowObjectIsValid
  - [update_frameheadpos](../u/update_frameheadpos.md)
  - [update_frametailpos](../u/update_frametailpos.md)
  - [update_grouptailpos](../u/update_grouptailpos.md)
  - [window_gettupleslot](../w/window_gettupleslot.md)
  - [row_is_in_frame](../r/row_is_in_frame.md)
  - [WinSetMarkPosition](WinSetMarkPosition.md)
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - [list_nth](../l/list_nth.md)
- Called from (representative examples):
  - [window_first_value](../w/window_first_value.md)
  - [window_last_value](../w/window_last_value.md)
  - [window_nth_value](../w/window_nth_value.md)

## Notes and Other Information
- WINDOW_SEEK_CURRENT is explicitly not supported due to ambiguous semantics when the current row might not be in the frame
- Exclusion clause handling is extremely sophisticated, accounting for peer groups and frame boundaries
- The mark position is adjusted intelligently when exclusion clauses are active to ensure safe sequential access patterns
- Non-existent or out-of-frame rows return null without raising errors
- Essential for implementing frame-aware window functions like FIRST_VALUE, LAST_VALUE, and NTH_VALUE
- Handles the complex interaction between window frames and exclusion options as defined in the SQL standard
- Performance optimization through mark positioning assumes monotonically increasing relpos in successive calls

## Simplified Source

```c
Datum
WinGetFuncArgInFrame(WindowObject winobj, int argno, int relpos, int seektype,
                    bool set_mark, bool *isnull, bool *isout)
{
    WindowAggState *winstate;
    ExprContext *econtext;
    TupleTableSlot *slot;
    int64 abs_pos, mark_pos;

    // Validate window object and get state
    Assert(WindowObjectIsValid(winobj));
    winstate = winobj->winstate;
    econtext = winstate->ss.ps.ps_ExprContext;
    slot = winstate->temp_slot_1;

    // Calculate position based on seek type
    switch (seektype)
    {
        case WINDOW_SEEK_CURRENT:
            elog(ERROR, "WINDOW_SEEK_CURRENT is not supported for WinGetFuncArgInFrame");
            break;

        case WINDOW_SEEK_HEAD:
            if (relpos < 0)
                goto out_of_frame;
            update_frameheadpos(winstate);
            abs_pos = winstate->frameheadpos + relpos;
            mark_pos = abs_pos;

            // Apply exclusion clause adjustments (simplified)
            switch (winstate->frameOptions & FRAMEOPTION_EXCLUSION)
            {
                case FRAMEOPTION_EXCLUDE_CURRENT_ROW:
                    if (abs_pos >= winstate->currentpos &&
                        winstate->currentpos >= winstate->frameheadpos)
                        abs_pos++;
                    break;
                // Additional exclusion cases handled similarly...
            }
            break;

        case WINDOW_SEEK_TAIL:
            if (relpos > 0)
                goto out_of_frame;
            update_frametailpos(winstate);
            abs_pos = winstate->frametailpos - 1 + relpos;
            mark_pos = winstate->frameheadpos;  // Safe mark position

            // Apply exclusion clause adjustments (simplified)
            // Complex exclusion logic omitted for brevity
            break;

        default:
            elog(ERROR, "unrecognized window seek type: %d", seektype);
            break;
    }

    // Try to get the tuple and verify it's in frame
    if (!window_gettupleslot(winobj, abs_pos, slot))
        goto out_of_frame;

    if (row_is_in_frame(winstate, abs_pos, slot) <= 0)
        goto out_of_frame;

    // Success: evaluate the argument expression
    if (isout)
        *isout = false;
    if (set_mark)
        WinSetMarkPosition(winobj, mark_pos);

    econtext->ecxt_outertuple = slot;
    return ExecEvalExpr((ExprState *) list_nth(winobj->argstates, argno),
                       econtext, isnull);

out_of_frame:
    if (isout)
        *isout = true;
    *isnull = true;
    return (Datum) 0;
}
```