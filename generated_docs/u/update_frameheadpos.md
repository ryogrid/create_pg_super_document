# update_frameheadpos

## Location
[src/backend/executor/nodeWindowAgg.c:1485-1734](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L1485-L1734)

## Overview
A static function that computes and sets the frame head position for the current row in window function processing, handling various frame boundary modes including UNBOUNDED PRECEDING, CURRENT ROW, and offset-based frames.

## Definition

```c
static void
update_frameheadpos(WindowAggState *winstate)
```
## Detailed Description
This function is responsible for determining where the window frame begins relative to the current row being processed in a window aggregation operation. It handles all supported frame start boundary types defined by SQL window functions:

- **UNBOUNDED PRECEDING**: Frame always starts at row 0 (first row of partition)
- **CURRENT ROW**: Frame starts at current row or first peer depending on frame mode
- **OFFSET-based frames**: Frame starts at a specific offset from current row

The function operates differently based on the frame mode:
- **ROWS mode**: Treats each row individually, computing physical row positions
- **RANGE mode**: Groups logically equivalent rows using ORDER BY column values and in_range functions
- **GROUPS mode**: Works with peer groups, where rows with identical ORDER BY values form a group

The function uses tuple stores to buffer partition data and maintains frame head position tracking through  and . It employs memory context switching for proper memory management and may trigger tuple spooling when accessing rows beyond the current buffer.

## Parameters / Member Variables
- `*winstate`: WindowAggState pointer containing all window function execution state including:
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory management)
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md) (tuple store navigation)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md) (tuple retrieval)
  - [are_peers](../a/are_peers.md) (peer comparison for RANGE/GROUPS modes)
  - [spool_tuples](../s/spool_tuples.md) (tuple buffering)
  - [slot_getattr](../s/slot_getattr.md) (tuple attribute extraction)
  - [FunctionCall5Coll](../F/FunctionCall5Coll.md) (in_range function calls for RANGE mode)
  - [ExecCopySlot](../E/ExecCopySlot.md)/ExecClearTuple (tuple slot management)
  - [DatumGetInt64](../D/DatumGetInt64.md) (offset value extraction)
- Called from (representative examples):
  - [eval_windowaggregates](../e/eval_windowaggregates.md) (during aggregate window function evaluation)
  - [row_is_in_frame](../r/row_is_in_frame.md) (for frame membership testing)
  - [ExecWindowAgg](../E/ExecWindowAgg.md) (main window aggregation execution)
  - [WinGetFuncArgInFrame](../W/WinGetFuncArgInFrame.md) (window function argument retrieval)

## Notes and Other Information
- The function may clobber  during GROUPS mode processing
- Frame head position computation ignores window exclusion clauses - exclusion is applied later
- Uses short-lived memory context switching to prevent memory leaks in repeated calls
- Maintains frame head validity flags to avoid redundant computation for the same row
- For RANGE mode with single ORDER BY column, uses specialized in_range functions for boundary checking
- Handles NULL values in ORDER BY columns according to NULLS FIRST/LAST specifications
- Critical for window function performance as it determines the starting boundary for all frame-based computations

## Simplified Source

```c
static void
update_frameheadpos(WindowAggState *winstate)
{
    WindowAgg *node = (WindowAgg *) winstate->ss.ps.plan;
    int frameOptions = winstate->frameOptions;

    // Return if position already computed for current row
    if (winstate->framehead_valid)
        return;

    // Switch to persistent memory context
    MemoryContext oldcontext = MemoryContextSwitchTo(
        winstate->ss.ps.ps_ExprContext->ecxt_per_query_memory);

    if (frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) {
        // Frame always starts at beginning of partition
        winstate->frameheadpos = 0;
        winstate->framehead_valid = true;
    }
    else if (frameOptions & FRAMEOPTION_START_CURRENT_ROW) {
        if (frameOptions & FRAMEOPTION_ROWS) {
            // In ROWS mode, head is current row
            winstate->frameheadpos = winstate->currentpos;
            winstate->framehead_valid = true;
        }
        else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) {
            // If no ORDER BY, all rows are peers
            if (node->ordNumCols == 0) {
                winstate->frameheadpos = 0;
                winstate->framehead_valid = true;
                MemoryContextSwitchTo(oldcontext);
                return;
            }

            // Find first peer of current row
            tuplestore_select_read_pointer(winstate->buffer, winstate->framehead_ptr);

            // Initialize frame head slot if needed
            if (winstate->frameheadpos == 0 && TupIsNull(winstate->framehead_slot)) {
                tuplestore_gettupleslot(winstate->buffer, true, true, winstate->framehead_slot);
            }

            // Advance until we find a peer of current row
            while (!TupIsNull(winstate->framehead_slot)) {
                if (are_peers(winstate, winstate->framehead_slot, winstate->ss.ss_ScanTupleSlot))
                    break;

                winstate->frameheadpos++;
                spool_tuples(winstate, winstate->frameheadpos);
                if (!tuplestore_gettupleslot(winstate->buffer, true, true, winstate->framehead_slot))
                    break;
            }
            winstate->framehead_valid = true;
        }
    }
    else if (frameOptions & FRAMEOPTION_START_OFFSET) {
        if (frameOptions & FRAMEOPTION_ROWS) {
            // Calculate physical offset from current row
            int64 offset = DatumGetInt64(winstate->startOffsetValue);
            if (frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING)
                offset = -offset;

            winstate->frameheadpos = winstate->currentpos + offset;

            // Ensure position stays within partition bounds
            if (winstate->frameheadpos < 0)
                winstate->frameheadpos = 0;
            else if (winstate->frameheadpos > winstate->currentpos + 1) {
                spool_tuples(winstate, winstate->frameheadpos - 1);
                if (winstate->frameheadpos > winstate->spooled_rows)
                    winstate->frameheadpos = winstate->spooled_rows;
            }
            winstate->framehead_valid = true;
        }
        else if (frameOptions & FRAMEOPTION_RANGE) {
            // Use in_range function for RANGE offset mode
            int sortCol = node->ordColIdx[0];
            bool sub = (frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING);
            bool less = false;

            // Flip flags if descending sort order
            if (!winstate->inRangeAsc) {
                sub = !sub;
                less = true;
            }

            tuplestore_select_read_pointer(winstate->buffer, winstate->framehead_ptr);

            // Initialize if needed
            if (winstate->frameheadpos == 0 && TupIsNull(winstate->framehead_slot)) {
                tuplestore_gettupleslot(winstate->buffer, true, true, winstate->framehead_slot);
            }

            // Find first row satisfying in_range constraint
            while (!TupIsNull(winstate->framehead_slot)) {
                Datum headval = slot_getattr(winstate->framehead_slot, sortCol, &headisnull);
                Datum currval = slot_getattr(winstate->ss.ss_ScanTupleSlot, sortCol, &currisnull);

                // Handle NULL values or call in_range function
                if (!headisnull && !currisnull) {
                    if (DatumGetBool(FunctionCall5Coll(&winstate->startInRangeFunc,
                                                       winstate->inRangeColl,
                                                       headval, currval,
                                                       winstate->startOffsetValue,
                                                       BoolGetDatum(sub),
                                                       BoolGetDatum(less))))
                        break;
                }

                winstate->frameheadpos++;
                spool_tuples(winstate, winstate->frameheadpos);
                if (!tuplestore_gettupleslot(winstate->buffer, true, true, winstate->framehead_slot))
                    break;
            }
            winstate->framehead_valid = true;
        }
        else if (frameOptions & FRAMEOPTION_GROUPS) {
            // Groups mode: find first row of target peer group
            int64 offset = DatumGetInt64(winstate->startOffsetValue);
            int64 target_group = (frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING)
                               ? winstate->currentgroup - offset
                               : winstate->currentgroup + offset;

            tuplestore_select_read_pointer(winstate->buffer, winstate->framehead_ptr);

            // Initialize if needed
            if (winstate->frameheadpos == 0 && TupIsNull(winstate->framehead_slot)) {
                tuplestore_gettupleslot(winstate->buffer, true, true, winstate->framehead_slot);
            }

            // Advance to target group
            while (!TupIsNull(winstate->framehead_slot)) {
                if (winstate->frameheadgroup >= target_group)
                    break;

                ExecCopySlot(winstate->temp_slot_2, winstate->framehead_slot);
                winstate->frameheadpos++;
                spool_tuples(winstate, winstate->frameheadpos);

                if (!tuplestore_gettupleslot(winstate->buffer, true, true, winstate->framehead_slot))
                    break;

                if (!are_peers(winstate, winstate->temp_slot_2, winstate->framehead_slot))
                    winstate->frameheadgroup++;
            }
            ExecClearTuple(winstate->temp_slot_2);
            winstate->framehead_valid = true;
        }
    }

    MemoryContextSwitchTo(oldcontext);
}
```