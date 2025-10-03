# update_grouptailpos

## Location
[src/backend/executor/nodeWindowAgg.c:1985-2045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L1985-L2045)

## Overview
A static function that computes and sets the group tail position for the current row in window function processing, identifying the first row after the current peer group for GROUPS-based frame operations.

## Definition

```c
static void
update_grouptailpos(WindowAggState *winstate)
```
## Detailed Description
This function is specifically designed to support GROUPS frame mode processing by determining where the current row's peer group ends. In GROUPS mode, rows with identical values in all ORDER BY columns are considered peers and form a single group. The group tail position points to the first row that is *not* a peer of the current row.

Unlike the frame head/tail functions which handle multiple frame modes, this function focuses exclusively on peer group boundary detection. It operates with the assumption that  is reset only when the current row advances into a new peer group, which means the function always needs to advance the group tail position by at least one row from its previous position.

The function implements an optimized approach:
- When there's no ORDER BY clause, all rows are peers, so group tail is set to end of partition
- When ORDER BY exists, it advances through rows starting from the last known group tail position
- It stops when it finds a row that is not a peer of the current row
- Unlike frame tail tracking, it doesn't need persistent storage since group tail is always advanced

## Parameters / Member Variables
- `*winstate`: WindowAggState pointer containing window function execution state including:
## Dependencies
- Functions called/Symbols referenced:
  - [spool_tuples](../s/spool_tuples.md) (ensures tuple availability for group boundary detection)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory management)
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md) (tuple store navigation)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md) (tuple retrieval from buffer)
  - [are_peers](../a/are_peers.md) (comparison of rows for peer group membership)
  - [ExecClearTuple](../E/ExecClearTuple.md) (cleanup of temporary tuple slot)
- Called from (representative examples):
  - [row_is_in_frame](../r/row_is_in_frame.md) (for GROUPS frame processing and exclusion)
  - [ExecWindowAgg](../E/ExecWindowAgg.md) (main window aggregation execution)
  - [WinGetFuncArgInFrame](../W/WinGetFuncArgInFrame.md) (window function argument retrieval in GROUPS mode)

## Notes and Other Information
- Function may clobber  during peer comparison operations
- Optimized for the fact that group tail position always advances (never needs to go backward)
- Does not require persistent group tail row storage unlike frame tail tracking
- Only meaningful when ORDER BY clause exists - without it, all rows form one large peer group
- Critical for GROUPS frame mode performance as it defines peer group boundaries
- Used both for frame boundary computation and window function exclusion processing
- The grouptail_valid flag optimization prevents redundant computation for the same row

## Simplified Source

```c
static void
update_grouptailpos(WindowAggState *winstate)
{
    WindowAgg *node = (WindowAgg *) winstate->ss.ps.plan;

    // Return if position already computed for current row
    if (winstate->grouptail_valid)
        return;

    // Switch to persistent memory context
    MemoryContext oldcontext = MemoryContextSwitchTo(
        winstate->ss.ps.ps_ExprContext->ecxt_per_query_memory);

    // If no ORDER BY, all rows are peers (one large group)
    if (node->ordNumCols == 0) {
        spool_tuples(winstate, -1);
        winstate->grouptailpos = winstate->spooled_rows;
        winstate->grouptailpos_valid = true;
        MemoryContextSwitchTo(oldcontext);
        return;
    }

    // Find end of current peer group
    // grouptailpos always needs to advance from current position
    tuplestore_select_read_pointer(winstate->buffer, winstate->grouptail_ptr);

    for (;;) {
        // Advance to next row
        winstate->grouptailpos++;
        spool_tuples(winstate, winstate->grouptailpos);

        // Try to fetch next tuple
        if (!tuplestore_gettupleslot(winstate->buffer, true, true, winstate->temp_slot_2))
            break;  // End of partition

        // Check if this row is still a peer of current row
        if (winstate->grouptailpos > winstate->currentpos &&
            !are_peers(winstate, winstate->temp_slot_2, winstate->ss.ss_ScanTupleSlot))
            break;  // Found first non-peer
    }

    ExecClearTuple(winstate->temp_slot_2);
    winstate->grouptail_valid = true;
    MemoryContextSwitchTo(oldcontext);
}
```