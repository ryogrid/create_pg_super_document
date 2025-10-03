# WinSetMarkPosition

## Location
[src/backend/executor/nodeWindowAgg.c:3218-3252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L3218-L3252)

## Overview
Sets the "mark" position for a WindowObject, defining the oldest row that can be fetched during subsequent operations within the current partition.

## Definition

```c
void
WinSetMarkPosition(WindowObject winobj, int64 markpos)
```
## Detailed Description
This function establishes a lower bound for tuple access within a partition by setting the mark position to a specified row number. The mark position represents the oldest row (by zero-based position) that the window function is allowed to fetch during subsequent operations. This mechanism serves as both a memory optimization and access control feature - by advancing the mark position forward, window functions can help keep the tuplestore size manageable and prevent unnecessary spilling to disk. The function ensures that the mark can only move forward (never backward) and automatically adjusts both the mark pointer and read pointer positions within the tuplestore to maintain consistency.

## Parameters / Member Variables
- : WindowObject containing window state, mark pointer, read pointer, and position tracking information
- : Zero-based position of the new mark (must be >= current mark position)

## Dependencies
- Functions called/Symbols referenced:
  - WindowObjectIsValid
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md)
  - [tuplestore_skiptuples](../t/tuplestore_skiptuples.md)
- Called from (representative examples):
  - [eval_windowaggregates](../e/eval_windowaggregates.md)
  - [WinGetFuncArgInPartition](WinGetFuncArgInPartition.md)
  - [WinGetFuncArgInFrame](WinGetFuncArgInFrame.md)
  - [rank_up](../r/rank_up.md)
  - [window_row_number](../w/window_row_number.md)

## Notes and Other Information
- Mark position can only move forward, never backward (enforced with error check)
- Helps optimize memory usage by allowing tuplestore to discard older tuples
- Updates both mark pointer and read pointer positions when necessary
- Optional for window functions but recommended for memory efficiency
- Prevents access to rows before the mark position via window_gettupleslot
- Uses tuplestore_skiptuples to efficiently advance pointer positions
- Part of the window function memory management strategy

## Simplified Source

```c
void
WinSetMarkPosition(WindowObject winobj, int64 markpos)
{
    WindowAggState *winstate;

    Assert(WindowObjectIsValid(winobj));
    winstate = winobj->winstate;

    // Ensure mark can only move forward
    if (markpos < winobj->markpos)
        elog(ERROR, "cannot move WindowObject's mark position backward");

    // Update mark pointer position if moving forward
    tuplestore_select_read_pointer(winstate->buffer, winobj->markptr);
    if (markpos > winobj->markpos)
    {
        tuplestore_skiptuples(winstate->buffer,
                             markpos - winobj->markpos,
                             true);
        winobj->markpos = markpos;
    }

    // Update read pointer position if needed
    tuplestore_select_read_pointer(winstate->buffer, winobj->readptr);
    if (markpos > winobj->seekpos)
    {
        tuplestore_skiptuples(winstate->buffer,
                             markpos - winobj->seekpos,
                             true);
        winobj->seekpos = markpos;
    }
}
```