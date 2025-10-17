# rank_up

## Location
[src/backend/utils/adt/windowfuncs.c:49-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L49-L83)

## Overview
A utility function that determines whether the rank should increase for window ranking functions by comparing the current row with the previous row based on the ORDER BY clause.

## Definition

```c
static bool
rank_up(WindowObject winobj)
```
## Detailed Description
This function is a core utility for PostgreSQL's ranking window functions (RANK, DENSE_RANK, PERCENT_RANK, etc.). It maintains the ranking context for each partition and determines when the rank value should be incremented. The function compares the current row with the previous row using the ORDER BY clause specified in the window function. If the rows are not peers (i.e., they have different values for the ORDER BY columns), the rank should increase.

The function uses partition-local memory to maintain a  structure that tracks the current rank value. On the first call for a partition, it initializes the rank to 1. For subsequent calls, it compares consecutive rows to determine if they are peers using .

## Parameters / Member Variables
- `winobj`: A WindowObject that provides access to the window frame, current position, and partition data
## Dependencies
- Functions called/Symbols referenced:
  - [WinGetCurrentPosition](../W/WinGetCurrentPosition.md)
  - [WinGetPartitionLocalMemory](../W/WinGetPartitionLocalMemory.md)
  - [WinRowsArePeers](../W/WinRowsArePeers.md)
  - [WinSetMarkPosition](../W/WinSetMarkPosition.md)
  - [rank_context](rank_context.md) (struct)
  - [WindowObject](../W/WindowObject.md) (type)
- Called from (representative examples):
  - [window_rank](../w/window_rank.md)
  - [window_dense_rank](../w/window_dense_rank.md)
  - [window_percent_rank](../w/window_percent_rank.md)
  - [window_cume_dist](../w/window_cume_dist.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the windowfuncs.c file
- The function manages the mark position to ensure proper access to the previous row before advancing
- The rank context is stored in partition-local memory, ensuring each partition maintains its own ranking state
- The function returns true when the rank should increase, false when the current row has the same rank as the previous row (peers)

## Simplified Source

```c
static bool rank_up(WindowObject winobj)
{
    int64 curpos = WinGetCurrentPosition(winobj);
    rank_context *context = (rank_context *)
        WinGetPartitionLocalMemory(winobj, sizeof(rank_context));

    if (context->rank == 0) {
        // First call: initialize rank to 1
        context->rank = 1;
        WinSetMarkPosition(winobj, curpos);
        return false;  // No rank increase on first row
    }

    // Check if current row differs from previous row
    bool should_increase = !WinRowsArePeers(winobj, curpos - 1, curpos);

    // Advance mark position after comparing with prior row
    WinSetMarkPosition(winobj, curpos);

    return should_increase;
}
```