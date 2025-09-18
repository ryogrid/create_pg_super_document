# WinSetMarkPosition

## Location
src/backend/executor/nodeWindowAgg.c: 3218 - 3252

## Overview
Sets the "mark" position for a WindowObject, defining the oldest row that can be fetched during subsequent operations within the current partition.

## Definition


## Detailed Description
This function establishes a lower bound for tuple access within a partition by setting the mark position to a specified row number. The mark position represents the oldest row (by zero-based position) that the window function is allowed to fetch during subsequent operations. This mechanism serves as both a memory optimization and access control feature - by advancing the mark position forward, window functions can help keep the tuplestore size manageable and prevent unnecessary spilling to disk. The function ensures that the mark can only move forward (never backward) and automatically adjusts both the mark pointer and read pointer positions within the tuplestore to maintain consistency.

## Parameters / Member Variables
- : WindowObject containing window state, mark pointer, read pointer, and position tracking information
- : Zero-based position of the new mark (must be >= current mark position)

## Dependencies
- Functions called/Symbols referenced:
  - WindowObjectIsValid
  - tuplestore_select_read_pointer
  - tuplestore_skiptuples
- Called from (representative examples):
  - eval_windowaggregates
  - WinGetFuncArgInPartition
  - WinGetFuncArgInFrame
  - rank_up
  - window_row_number

## Notes and Other Information
- Mark position can only move forward, never backward (enforced with error check)
- Helps optimize memory usage by allowing tuplestore to discard older tuples
- Updates both mark pointer and read pointer positions when necessary
- Optional for window functions but recommended for memory efficiency
- Prevents access to rows before the mark position via window_gettupleslot
- Uses tuplestore_skiptuples to efficiently advance pointer positions
- Part of the window function memory management strategy