# WinGetCurrentPosition

## Location
src/backend/executor/nodeWindowAgg.c: 3185 - 3199

## Overview
Returns the current row's zero-based position within the current partition during window function evaluation.

## Definition


## Detailed Description
This function provides a simple interface for window functions to determine the current row's position within the partition being processed. It returns a zero-based index that represents the ordinal position of the row currently being evaluated by the window function. The position counter is maintained by the window aggregate state machinery and is automatically updated as the window function evaluation progresses through the partition. This is a fundamental building block for position-dependent window functions like ROW_NUMBER, RANK, and statistical functions that need to know their relative position within the partition.

## Parameters / Member Variables
- : WindowObject containing the window state and current position information

## Dependencies
- Functions called/Symbols referenced:
  - WindowObjectIsValid
- Called from (representative examples):
  - [rank_up](../r/rank_up.md)
  - [window_row_number](../w/window_row_number.md)
  - [window_rank](../w/window_rank.md)
  - [window_percent_rank](../w/window_percent_rank.md)
  - [window_cume_dist](../w/window_cume_dist.md)

## Notes and Other Information
- Returns int64 representing the zero-based position of the current row
- Position is relative to the current partition, not the entire result set
- Position counter is maintained automatically by the WindowAgg executor node
- Essential for implementing ranking and numbering window functions
- Validates WindowObject before accessing position information
- Position starts at 0 for the first row in each partition