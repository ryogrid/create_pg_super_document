# WinRowsArePeers

## Location
[src/backend/executor/nodeWindowAgg.c:3253-3309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L3253-L3309)

## Overview
Compares two rows at specified absolute positions within a window partition to determine if they are equal according to the ORDER BY clause of the window function.

## Definition

```c
bool
WinRowsArePeers(WindowObject winobj, int64 pos1, int64 pos2)
```
## Detailed Description
This function is a core component of PostgreSQL's window function implementation that determines peer relationships between rows within a window partition. It performs row-by-row comparison based solely on the ORDER BY columns of the window specification, ignoring any frame mode considerations.

The function operates by:
1. Validating the window object and extracting the associated window state
2. Short-circuiting to return true if no ORDER BY clause exists (all rows are peers)
3. Fetching the tuples at the specified positions using temporary slots
4. Performing the actual peer comparison using the  helper function
5. Cleaning up temporary tuple slots before returning

This is essential for window functions that need to understand row equivalence, such as ranking functions and cumulative distribution calculations.

## Parameters / Member Variables
- : Window object containing the partition data and window state
- : Absolute position of the first row within the partition (0-based)
- : Absolute position of the second row within the partition (0-based)

## Dependencies
- Functions called/Symbols referenced:
  - WindowObjectIsValid
  - [window_gettupleslot](../w/window_gettupleslot.md)
  - [are_peers](../a/are_peers.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
- Called from (representative examples):
  - [rank_up](../r/rank_up.md)
  - [window_cume_dist](../w/window_cume_dist.md)

## Notes and Other Information
- The function does not consider window frame modes - it only compares based on ORDER BY columns
- Uses temporary tuple slots (temp_slot_1 and temp_slot_2) for efficient tuple comparison
- Raises ERROR if either specified position is outside the window bounds
- Returns true immediately if no ORDER BY clause exists, treating all rows as peers
- Critical for implementing SQL window functions that depend on row ordering semantics