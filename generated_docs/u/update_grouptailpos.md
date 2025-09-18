# update_grouptailpos

## Location
src/backend/executor/nodeWindowAgg.c: 1985 - 2045

## Overview
A static function that computes and sets the group tail position for the current row in window function processing, identifying the first row after the current peer group for GROUPS-based frame operations.

## Definition


## Detailed Description
This function is specifically designed to support GROUPS frame mode processing by determining where the current row's peer group ends. In GROUPS mode, rows with identical values in all ORDER BY columns are considered peers and form a single group. The group tail position points to the first row that is *not* a peer of the current row.

Unlike the frame head/tail functions which handle multiple frame modes, this function focuses exclusively on peer group boundary detection. It operates with the assumption that  is reset only when the current row advances into a new peer group, which means the function always needs to advance the group tail position by at least one row from its previous position.

The function implements an optimized approach:
- When there's no ORDER BY clause, all rows are peers, so group tail is set to end of partition
- When ORDER BY exists, it advances through rows starting from the last known group tail position
- It stops when it finds a row that is not a peer of the current row
- Unlike frame tail tracking, it doesn't need persistent storage since group tail is always advanced

## Parameters / Member Variables
- : WindowAggState pointer containing window function execution state including:
  - Current row position and peer group context
  - Tuple store for partition data buffering
  - Group tail position tracking and validation flags
  - Memory contexts and temporary tuple slots
  - ORDER BY column specifications for peer comparison

## Dependencies
- Functions called/Symbols referenced:
  - spool_tuples (ensures tuple availability for group boundary detection)
  - MemoryContextSwitchTo (memory management)
  - tuplestore_select_read_pointer (tuple store navigation)
  - tuplestore_gettupleslot (tuple retrieval from buffer)
  - are_peers (comparison of rows for peer group membership)
  - ExecClearTuple (cleanup of temporary tuple slot)
- Called from (representative examples):
  - row_is_in_frame (for GROUPS frame processing and exclusion)
  - ExecWindowAgg (main window aggregation execution)
  - WinGetFuncArgInFrame (window function argument retrieval in GROUPS mode)

## Notes and Other Information
- Function may clobber  during peer comparison operations
- Optimized for the fact that group tail position always advances (never needs to go backward)
- Does not require persistent group tail row storage unlike frame tail tracking
- Only meaningful when ORDER BY clause exists - without it, all rows form one large peer group
- Critical for GROUPS frame mode performance as it defines peer group boundaries
- Used both for frame boundary computation and window function exclusion processing
- The grouptail_valid flag optimization prevents redundant computation for the same row